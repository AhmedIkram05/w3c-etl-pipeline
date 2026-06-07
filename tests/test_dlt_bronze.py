"""
Unit tests for the Bronze DLT pipeline (``dlt_bronze.py``).

Tests cover the core logic of the W3C Auto Loader ingestion:

* ``_parse_file_content`` — per-file W3C format detection (CRIT-01 fix)
* ``_detect_format_from_content`` — 14-field vs 18-field header parsing
* ``_parse_log_line`` — inline copy, safe_int / safe_date reuse
* Streaming deduplication via ``ROW_NUMBER()`` over composite dedup key
* DLT ``@dlt.expect_or_drop`` constraint behaviour
* ``parse_file_udf`` — PySpark UDF wrapping ``_parse_file_content``
* Bronze table properties and partition config structural checks

All tests that exercise PySpark UDFs or DataFrames use the session-scoped
``spark`` fixture from ``conftest.py``.  Tests that validate pure-Python
helper functions do not require a SparkSession.
"""

from datetime import date
from typing import TYPE_CHECKING

import pytest
from pyspark.sql import Row
from pyspark.sql.functions import col, explode
from pyspark.sql.types import StringType, StructField, StructType

# ── Module under test ──────────────────────────────────────────────────
# dlt_bronze.py contains inline copies of safe_int / safe_date and the
# full _parse_log_line / _detect_format_from_content / _parse_file_content
# functions.  We import them directly to test in isolation.

if TYPE_CHECKING:
    from airflow.spark.databricks.dlt_bronze import (
        _detect_format_from_content,
        _parse_file_content,
        _parse_log_line,
        parse_file_udf,
        safe_date,
        safe_int,
    )
else:
    try:
        from airflow.spark.databricks.dlt_bronze import (
            _detect_format_from_content,
            _parse_file_content,
            _parse_log_line,
            parse_file_udf,
            safe_date,
            safe_int,
        )
    except ImportError:
        try:
            from databricks.dlt_bronze import (
                _detect_format_from_content,
                _parse_file_content,
                _parse_log_line,
                parse_file_udf,
                safe_date,
                safe_int,
            )
        except ImportError:
            from dlt_bronze import (
                _detect_format_from_content,
                _parse_file_content,
                _parse_log_line,
                parse_file_udf,
                safe_date,
                safe_int,
            )


# ══════════════════════════════════════════════════════════════════════
#  1. safe_int — inline copy from dlt_bronze
# ══════════════════════════════════════════════════════════════════════


class TestSafeInt:
    """Verify the inline ``safe_int`` helper in dlt_bronze.py."""

    def test_positive_int(self):
        assert safe_int("42") == 42

    def test_zero(self):
        assert safe_int("0") == 0

    def test_negative_becomes_none(self):
        assert safe_int("-1") is None

    def test_dash_placeholder(self):
        assert safe_int("-") is None

    def test_empty_string(self):
        assert safe_int("") is None

    def test_whitespace_string(self):
        assert safe_int("  ") is None

    def test_non_numeric(self):
        assert safe_int("abc") is None

    def test_none_input(self):
        assert safe_int(None) is None

    def test_float_string(self):
        """Float strings are not valid ints — return None."""
        assert safe_int("3.14") is None


# ══════════════════════════════════════════════════════════════════════
#  2. safe_date — inline copy from dlt_bronze
# ══════════════════════════════════════════════════════════════════════


class TestSafeDate:
    """Verify the inline ``safe_date`` helper in dlt_bronze.py."""

    def test_valid_iso_date(self):
        assert safe_date("2009-10-24") == date(2009, 10, 24)

    def test_dash_placeholder(self):
        assert safe_date("-") is None

    def test_empty_string(self):
        assert safe_date("") is None

    def test_invalid_format(self):
        assert safe_date("10/24/2009") is None

    def test_none_input(self):
        assert safe_date(None) is None


# ══════════════════════════════════════════════════════════════════════
#  3. _detect_format_from_content — CRIT-01: per-file header detection
# ══════════════════════════════════════════════════════════════════════


class TestDetectFormatFromContent:
    """Per-file format detection from #Fields: header (CRIT-01)."""

    def test_14_field_header(self):
        """14-field header returns 14."""
        content = b"#Fields: date time s-ip cs-method cs-uri-stem cs-uri-query s-port c-username c-ip cs(User-Agent) sc-status sc-substatus sc-win32-status sc-time-taken\n"
        assert _detect_format_from_content(content) == 14

    def test_18_field_header(self):
        """18-field header returns 18."""
        content = b"#Fields: date time s-ip cs-method cs-uri-stem cs-uri-query s-port c-username c-ip cs(User-Agent) cs(Cookie) cs(Referer) sc-status sc-substatus sc-win32-status sc-bytes cs-bytes sc-time-taken\n"
        assert _detect_format_from_content(content) == 18

    def test_no_header_defaults_to_18(self):
        """No #Fields: header → default to 18."""
        content = b"2009-10-24 01:40:40 134.36.36.75 GET / - 80 - 1.2.3.4 UA 200 0 0 186\n"
        assert _detect_format_from_content(content) == 18

    def test_empty_content_defaults_to_18(self):
        """Empty content → default to 18."""
        assert _detect_format_from_content(b"") == 18

    def test_header_with_comments(self):
        """#Fields: header among comment lines is still detected."""
        content = b"#Software: Microsoft Internet Information Services 7.0\n#Version: 1.0\n#Fields: date time s-ip cs-method cs-uri-stem cs-uri-query s-port c-username c-ip cs(User-Agent) sc-status sc-substatus sc-win32-status sc-time-taken\n#Date: 2009-10-24\n"
        assert _detect_format_from_content(content) == 14

    def test_malformed_bytes(self):
        """Non-UTF-8 content does not crash — returns default 18."""
        content = b"\xff\xfe\x00\x01\x02"
        assert _detect_format_from_content(content) == 18

    def test_header_with_extra_whitespace(self):
        """#Fields: line with irregular whitespace is parsed correctly."""
        content = b"#Fields:  date   time   s-ip   cs-method   cs-uri-stem \n"
        assert _detect_format_from_content(content) == 14


# ══════════════════════════════════════════════════════════════════════
#  4. _parse_log_line — 14-field format (inline copy)
# ══════════════════════════════════════════════════════════════════════


class TestParseLogLine14Field:
    """Parsing 14-field W3C log lines using the inline ``_parse_log_line``."""

    SAMPLE_14 = (
        "2009-10-24 01:40:40 134.36.36.75 HEAD / - 80 - 205.178.184.153 "
        "Mozilla/5.0+(Windows;+U;+Windows+NT+5.1;+en-US;+rv:1.8.1.12)+Gecko/20080201+Firefox/2.0.0.12 "
        "200 0 0 186"
    )

    SAMPLE_14_404 = (
        "2009-10-24 08:34:52 134.36.36.75 GET /abc.php auth=45V456b09m "
        "80 - 91.212.127.100 "
        "Mozilla/5.0+(Windows;+U;+Windows+NT+5.0;+en-US;+rv:1.8.1.12)+Gecko/20080201+Firefox/2.0.0.12 "
        "404 0 64 92"
    )

    def test_standard_get(self):
        result = _parse_log_line(self.SAMPLE_14, 14, "u_ex091024.log")
        assert result is not None
        assert result["log_date"] == date(2009, 10, 24)
        assert result["log_time"] == "01:40:40"
        assert result["server_ip"] == "134.36.36.75"
        assert result["method"] == "HEAD"
        assert result["uri_stem"] == "/"
        assert result["uri_query"] == "-"
        assert result["server_port"] == 80
        assert result["username"] == "-"
        assert result["client_ip"] == "205.178.184.153"
        assert "Firefox" in result["user_agent"]
        assert result["status"] == 200
        assert result["sub_status"] == 0
        assert result["win32_status"] == 0
        assert result["time_taken"] == 186
        assert result["source_file"] == "u_ex091024.log"
        # 14-field: optional fields default to None
        assert result["cookie"] is None
        assert result["referrer"] is None
        assert result["bytes_sent"] is None
        assert result["bytes_recv"] is None

    def test_with_query_string(self):
        result = _parse_log_line(self.SAMPLE_14_404, 14, "u_ex091024.log")
        assert result is not None
        assert result["method"] == "GET"
        assert result["uri_stem"] == "/abc.php"
        assert "auth=" in result["uri_query"]
        assert result["status"] == 404
        assert result["time_taken"] == 92

    def test_short_line_returns_none(self):
        assert _parse_log_line("too short", 14, "test.log") is None

    def test_empty_line_returns_none(self):
        assert _parse_log_line("", 14, "test.log") is None


# ══════════════════════════════════════════════════════════════════════
#  5. _parse_log_line — 18-field format (inline copy)
# ══════════════════════════════════════════════════════════════════════


class TestParseLogLine18Field:
    """Parsing 18-field W3C log lines using the inline ``_parse_log_line``."""

    SAMPLE_18 = (
        "2010-07-18 00:07:19 134.36.36.75 GET /Darwin/MyAccount.aspx "
        "p=25&pid=436&prcid=62&ppid=1505 80 - 216.129.119.45 "
        "Mozilla/5.0+(Twiceler-0.9+http://www.cuil.com/twiceler/robot.html) "
        "- - 200 0 0 6635 252 394"
    )

    SAMPLE_18_REF = (
        "2010-07-18 00:50:44 134.36.36.75 GET /Darwin/style.css - 80 - 67.195.113.235 "
        "Mozilla/5.0+(compatible;+Yahoo!+Slurp/3.0;+http://help.yahoo.com/help/us/ysearch/slurp) "
        "- http://www.example.org/page "
        "304 0 0 229 485 179"
    )

    def test_standard_get(self):
        result = _parse_log_line(self.SAMPLE_18, 18, "u_ex100718.log")
        assert result is not None
        assert result["log_date"] == date(2010, 7, 18)
        assert result["client_ip"] == "216.129.119.45"
        assert result["cookie"] == "-"
        assert result["referrer"] == "-"
        assert result["status"] == 200
        assert result["bytes_sent"] == 6635
        assert result["bytes_recv"] == 252
        assert result["time_taken"] == 394

    def test_with_referrer(self):
        result = _parse_log_line(self.SAMPLE_18_REF, 18, "u_ex100718.log")
        assert result is not None
        assert result["referrer"] == "http://www.example.org/page"
        assert result["status"] == 304
        assert result["bytes_sent"] == 229
        assert result["bytes_recv"] == 485

    def test_short_line_returns_none(self):
        assert _parse_log_line("too short", 18, "test.log") is None

    def test_empty_line_returns_none(self):
        assert _parse_log_line("", 18, "test.log") is None

    def test_dash_bytes_return_none(self):
        """Dashes in numeric byte fields become None."""
        line = "2010-07-18 00:00:00 1.2.3.4 GET / - 80 - 5.6.7.8 UA - - 200 0 0 - - 100"
        result = _parse_log_line(line, 18, "test.log")
        assert result is not None
        assert result["bytes_sent"] is None
        assert result["bytes_recv"] is None
        assert result["time_taken"] == 100


# ══════════════════════════════════════════════════════════════════════
#  6. _parse_log_line — edge cases
# ══════════════════════════════════════════════════════════════════════


class TestParseLogLineEdgeCases:
    """Edge cases for the inline ``_parse_log_line``."""

    def test_unknown_format_returns_none(self):
        """A format value that is neither 14 nor 18 returns None."""
        line = "2009-10-24 01:40:40 134.36.36.75 GET / - 80 - 1.2.3.4 UA 200 0 0 100"
        assert _parse_log_line(line, 99, "test.log") is None

    def test_negative_time_taken(self):
        """Negative time_taken → safe_int returns None."""
        line = "2009-10-24 01:40:40 134.36.36.75 GET / - 80 - 1.2.3.4 TestBot 200 0 0 -1"
        result = _parse_log_line(line, 14, "test.log")
        assert result is not None
        assert result["time_taken"] is None

    def test_negative_bytes_sent(self):
        """Negative bytes_sent → safe_int returns None."""
        line = "2010-07-18 00:00:00 1.2.3.4 GET / - 80 - 5.6.7.8 UA - - 200 0 0 -5 100 50"
        result = _parse_log_line(line, 18, "test.log")
        assert result is not None
        assert result["bytes_sent"] is None

    def test_invalid_date(self):
        """Non-date in the date field → safe_date returns None → log_date is None."""
        line = "INVALID_DATE 01:40:40 134.36.36.75 GET / - 80 - 1.2.3.4 UA 200 0 0 100"
        result = _parse_log_line(line, 14, "test.log")
        assert result is not None
        assert result["log_date"] is None

    def test_non_numeric_port(self):
        """Non-numeric server_port → safe_int returns None."""
        line = "2009-10-24 01:40:40 134.36.36.75 GET / - abc - 1.2.3.4 UA 200 0 0 100"
        result = _parse_log_line(line, 14, "test.log")
        assert result is not None
        assert result["server_port"] is None


# ══════════════════════════════════════════════════════════════════════
#  7. _parse_file_content — full file parsing with auto-format detection
# ══════════════════════════════════════════════════════════════════════


class TestParseFileContent:
    """Exercise the per-file UDF that detects format then parses lines."""

    def test_14_field_file(self):
        """Parse a full 14-field file with #Fields: header."""
        content = (
            b"#Fields: date time s-ip cs-method cs-uri-stem cs-uri-query "
            b"s-port c-username c-ip cs(User-Agent) sc-status sc-substatus "
            b"sc-win32-status sc-time-taken\n"
            b"2009-10-24 01:40:40 134.36.36.75 GET / - 80 - 1.2.3.4 Mozilla/5.0 200 0 0 100\n"
            b"2009-10-24 01:41:00 134.36.36.75 POST /login - 80 - 4.3.2.1 Mozilla/5.0 302 0 0 50\n"
        )
        results = _parse_file_content(content, "u_ex091024.log")
        assert len(results) == 2
        assert results[0]["log_date"] == date(2009, 10, 24)
        assert results[0]["method"] == "GET"
        assert results[0]["client_ip"] == "1.2.3.4"
        assert results[1]["method"] == "POST"
        assert results[1]["status"] == 302

    def test_18_field_file(self):
        """Parse a full 18-field file with #Fields: header."""
        content = (
            b"#Software: Microsoft Internet Information Services 7.0\n"
            b"#Version: 1.0\n"
            b"#Fields: date time s-ip cs-method cs-uri-stem cs-uri-query "
            b"s-port c-username c-ip cs(User-Agent) cs(Cookie) cs(Referer) "
            b"sc-status sc-substatus sc-win32-status sc-bytes cs-bytes sc-time-taken\n"
            b"2010-07-18 00:07:19 134.36.36.75 GET /page.aspx p=1 80 - 216.129.119.45 "
            b"Mozilla/5.0 - - 200 0 0 6635 252 394\n"
        )
        results = _parse_file_content(content, "u_ex100718.log")
        assert len(results) == 1
        assert results[0]["bytes_sent"] == 6635
        assert results[0]["bytes_recv"] == 252

    def test_comment_lines_skipped(self):
        """Comment lines (#) and blank lines are ignored."""
        content = (
            b"#Fields: date time s-ip cs-method cs-uri-stem cs-uri-query "
            b"s-port c-username c-ip cs(User-Agent) sc-status sc-substatus "
            b"sc-win32-status sc-time-taken\n"
            b"#Software: IIS\n"
            b"#Date: 2009-10-24\n"
            b"\n"
            b"2009-10-24 01:00:00 1.2.3.4 GET / - 80 - 5.6.7.8 UA 200 0 0 100\n"
            b"\n"
            b"2009-10-24 02:00:00 1.2.3.4 GET /about - 80 - 5.6.7.8 UA 200 0 0 50\n"
        )
        results = _parse_file_content(content, "test.log")
        assert len(results) == 2

    def test_malformed_line_skipped(self):
        """A malformed log line is skipped (not fatal)."""
        content = (
            b"#Fields: date time s-ip cs-method cs-uri-stem cs-uri-query "
            b"s-port c-username c-ip cs(User-Agent) sc-status sc-substatus "
            b"sc-win32-status sc-time-taken\n"
            b"2009-10-24 01:00:00 1.2.3.4 GET / - 80 - 5.6.7.8 UA 200 0 0 100\n"
            b"this line is too short\n"
            b"2009-10-24 02:00:00 1.2.3.4 GET /about - 80 - 5.6.7.8 UA 200 0 0 50\n"
        )
        results = _parse_file_content(content, "test.log")
        assert len(results) == 2

    def test_empty_content_returns_empty_list(self):
        assert _parse_file_content(b"", "empty.log") == []

    def test_non_utf8_content_returns_empty_list(self):
        """Binary data that can't be decoded returns empty list."""
        assert _parse_file_content(b"\xff\xfe\x00\x01", "bad.log") == []

    def test_only_comments_returns_empty_list(self):
        content = b"#Software: IIS\n#Version: 1.0\n#Date: 2009-10-24\n"
        results = _parse_file_content(content, "comments.log")
        assert results == []

    def test_format_detected_from_first_header(self):
        """Uses the first #Fields: line, ignores later ones if any."""
        content = (
            b"#Fields: date time s-ip cs-method cs-uri-stem cs-uri-query "
            b"s-port c-username c-ip cs(User-Agent) sc-status sc-substatus "
            b"sc-win32-status sc-time-taken\n"
            b"2009-10-24 01:00:00 1.2.3.4 GET / - 80 - 5.6.7.8 UA 200 0 0 100\n"
        )
        results = _parse_file_content(content, "test.log")
        assert len(results) == 1
        # If detected as 18, bytes_sent would be parsed from the line
        # and would NOT be None. It was detected as 14, so bytes_sent=None.
        assert results[0]["bytes_sent"] is None


# ══════════════════════════════════════════════════════════════════════
#  8. parse_file_udf — PySpark UDF integration
# ══════════════════════════════════════════════════════════════════════


class TestParseFileUDF:
    """Verify the ``parse_file_udf`` works as a PySpark UDF."""

    def test_udf_returns_array_of_structs(self, spark):
        """Applying the UDF to a single-row DataFrame returns parsed rows."""
        content = (
            b"#Fields: date time s-ip cs-method cs-uri-stem cs-uri-query "
            b"s-port c-username c-ip cs(User-Agent) sc-status sc-substatus "
            b"sc-win32-status sc-time-taken\n"
            b"2009-10-24 01:40:40 134.36.36.75 GET / - 80 - 1.2.3.4 Mozilla/5.0 200 0 0 100\n"
        )
        source_path = "u_ex091024.log"

        schema = StructType([
            StructField("content", StringType(), True),
            StructField("path", StringType(), True),
        ])
        df = spark.createDataFrame([Row(content=content, path=source_path)], schema=schema)

        result_df = df.select(explode(parse_file_udf(col("content"), col("path"))).alias("parsed"))
        rows = result_df.collect()
        assert len(rows) == 1
        assert rows[0]["parsed"]["log_date"] == date(2009, 10, 24)
        assert rows[0]["parsed"]["client_ip"] == "1.2.3.4"

    def test_udf_with_multiple_files(self, spark):
        """Multiple files in the DataFrame each produce correct parsed rows."""
        row1 = Row(
            content=(
                b"#Fields: date time s-ip cs-method cs-uri-stem cs-uri-query "
                b"s-port c-username c-ip cs(User-Agent) sc-status sc-substatus "
                b"sc-win32-status sc-time-taken\n"
                b"2009-10-24 01:00:00 1.2.3.4 GET / - 80 - 5.6.7.8 UA 200 0 0 100\n"
            ),
            path="file1.log",
        )
        row2 = Row(
            content=(
                b"#Fields: date time s-ip cs-method cs-uri-stem cs-uri-query "
                b"s-port c-username c-ip cs(User-Agent) cs(Cookie) cs(Referer) "
                b"sc-status sc-substatus sc-win32-status sc-bytes cs-bytes sc-time-taken\n"
                b"2010-07-18 00:00:00 1.2.3.4 GET /page - 80 - 9.8.7.6 UA - - 200 0 0 1000 500 200\n"
            ),
            path="file2.log",
        )
        df = spark.createDataFrame([row1, row2])

        result_df = df.select(explode(parse_file_udf(col("content"), col("path"))).alias("parsed"))
        rows = result_df.collect()
        assert len(rows) == 2

        ips = {r["parsed"]["client_ip"] for r in rows}
        assert ips == {"5.6.7.8", "9.8.7.6"}

        # file2 (18-field) has bytes_sent; file1 (14-field) does not
        bytes_values = {r["parsed"]["bytes_sent"] for r in rows}
        assert None in bytes_values  # 14-field file
        assert 1000 in bytes_values  # 18-field file

    def test_udf_with_empty_file_returns_no_rows(self, spark):
        """Empty file content produces zero exploded rows."""
        df = spark.createDataFrame([Row(content=b"", path="empty.log")])
        result_df = df.select(explode(parse_file_udf(col("content"), col("path"))).alias("parsed"))
        assert result_df.count() == 0


# ══════════════════════════════════════════════════════════════════════
#  9. Streaming deduplication logic (ROW_NUMBER over composite key)
# ══════════════════════════════════════════════════════════════════════


class TestStreamingDeduplication:
    """Verify the Bronze ROW_NUMBER dedup pattern using PySpark."""

    def test_exact_duplicate_removed(self, spark):
        """Two rows with identical dedup keys → only one survives."""
        from pyspark.sql.functions import col, concat_ws, row_number
        from pyspark.sql.window import Window

        data = [
            Row(source_file="f1", log_date=date(2009, 1, 1), log_time="00:00:00",
                client_ip="1.2.3.4", method="GET", uri_stem="/", status=200),
            Row(source_file="f1", log_date=date(2009, 1, 1), log_time="00:00:00",
                client_ip="1.2.3.4", method="GET", uri_stem="/", status=200),
        ]
        df = spark.createDataFrame(data)

        df = df.withColumn(
            "dedup_key",
            concat_ws("|", col("source_file"), col("log_date"), col("log_time"),
                      col("client_ip"), col("method"), col("uri_stem"), col("status"))
        )
        window_spec = Window.partitionBy("dedup_key").orderBy("source_file")
        df = df.withColumn("row_num", row_number().over(window_spec))
        df = df.filter(col("row_num") == 1).drop("row_num", "dedup_key")

        assert df.count() == 1

    def test_different_rows_preserved(self, spark):
        """Rows with different dedup keys are all preserved."""
        from pyspark.sql.functions import col, concat_ws, row_number
        from pyspark.sql.window import Window

        data = [
            Row(source_file="f1", log_date=date(2009, 1, 1), log_time="00:00:00",
                client_ip="1.2.3.4", method="GET", uri_stem="/a", status=200),
            Row(source_file="f1", log_date=date(2009, 1, 1), log_time="00:00:01",
                client_ip="1.2.3.4", method="GET", uri_stem="/b", status=200),
        ]
        df = spark.createDataFrame(data)

        df = df.withColumn(
            "dedup_key",
            concat_ws("|", col("source_file"), col("log_date"), col("log_time"),
                      col("client_ip"), col("method"), col("uri_stem"), col("status"))
        )
        window_spec = Window.partitionBy("dedup_key").orderBy("source_file")
        df = df.withColumn("row_num", row_number().over(window_spec))
        df = df.filter(col("row_num") == 1).drop("row_num", "dedup_key")

        assert df.count() == 2

    def test_same_event_different_source_files(self, spark):
        """Same event from different source files → both kept (different dedup_key)."""
        from pyspark.sql.functions import col, concat_ws, row_number
        from pyspark.sql.window import Window

        log_date = date(2009, 1, 1)
        data = [
            Row(source_file="f1", log_date=log_date, log_time="00:00:00",
                client_ip="1.2.3.4", method="GET", uri_stem="/", status=200),
            Row(source_file="f2", log_date=log_date, log_time="00:00:00",
                client_ip="1.2.3.4", method="GET", uri_stem="/", status=200),
        ]
        df = spark.createDataFrame(data)

        df = df.withColumn(
            "dedup_key",
            concat_ws("|", col("source_file"), col("log_date"), col("log_time"),
                      col("client_ip"), col("method"), col("uri_stem"), col("status"))
        )
        window_spec = Window.partitionBy("dedup_key").orderBy("source_file")
        df = df.withColumn("row_num", row_number().over(window_spec))
        df = df.filter(col("row_num") == 1).drop("row_num", "dedup_key")

        # source_file is part of dedup_key, so these are different rows
        assert df.count() == 2

    def test_dedup_key_concatenation(self, spark):
        """Verify the composite dedup_key is built from all 7 fields."""
        from pyspark.sql.functions import col, concat_ws
        from pyspark.sql.types import DateType

        df = spark.createDataFrame([
            Row(source_file="f1", log_date=date(2009, 1, 1), log_time="00:00:00",
                client_ip="1.2.3.4", method="GET", uri_stem="/", status=200)
        ])

        df = df.withColumn(
            "dedup_key",
            concat_ws("|", col("source_file"), col("log_date"), col("log_time"),
                      col("client_ip"), col("method"), col("uri_stem"), col("status"))
        )
        key = df.select("dedup_key").collect()[0][0]
        assert key == "f1|2009-01-01|00:00:00|1.2.3.4|GET|/|200"


# ══════════════════════════════════════════════════════════════════════
#  10. DLT expectations — structural constraint verification
# ══════════════════════════════════════════════════════════════════════


class TestBronzeExpectations:
    """Verify the DLT ``@dlt.expect_or_drop`` constraints via DataFrame filters.

    Since ``dlt`` is not importable outside a DLT runtime, we simulate
    each expectation as a filter on the parsed data so the test is hermetic.
    """

    def test_expect_valid_log_date(self, spark):
        """Rows with null log_date are dropped (valid_log_date)."""
        from pyspark.sql.functions import col

        df = spark.createDataFrame([
            Row(log_date=date(2009, 1, 1), status=200, client_ip="1.2.3.4",
                method="GET", uri_stem="/", user_agent="UA", bytes_sent=100, bytes_recv=0),
            Row(log_date=None, status=200, client_ip="1.2.3.4",
                method="GET", uri_stem="/", user_agent="UA", bytes_sent=100, bytes_recv=0),
        ])
        filtered = df.filter(col("log_date").isNotNull())
        assert filtered.count() == 1

    def test_expect_valid_status(self, spark):
        """Rows with status outside 100-599 are dropped (valid_status)."""
        from pyspark.sql.functions import col

        df = spark.createDataFrame([
            Row(log_date=date(2009, 1, 1), status=200, client_ip="1.2.3.4",
                method="GET", uri_stem="/", user_agent="UA", bytes_sent=100, bytes_recv=0),
            Row(log_date=date(2009, 1, 1), status=99, client_ip="1.2.3.4",
                method="GET", uri_stem="/", user_agent="UA", bytes_sent=100, bytes_recv=0),
            Row(log_date=date(2009, 1, 1), status=600, client_ip="1.2.3.4",
                method="GET", uri_stem="/", user_agent="UA", bytes_sent=100, bytes_recv=0),
            Row(log_date=date(2009, 1, 1), status=None, client_ip="1.2.3.4",
                method="GET", uri_stem="/", user_agent="UA", bytes_sent=100, bytes_recv=0),
        ])
        filtered = df.filter(col("status").between(100, 599))
        assert filtered.count() == 1

    def test_expect_valid_client_ip(self, spark):
        """Rows with null or '-' client_ip are dropped (valid_client_ip)."""
        from pyspark.sql.functions import col

        df = spark.createDataFrame([
            Row(log_date=date(2009, 1, 1), status=200, client_ip="1.2.3.4",
                method="GET", uri_stem="/", user_agent="UA", bytes_sent=100, bytes_recv=0),
            Row(log_date=date(2009, 1, 1), status=200, client_ip="-",
                method="GET", uri_stem="/", user_agent="UA", bytes_sent=100, bytes_recv=0),
            Row(log_date=date(2009, 1, 1), status=200, client_ip=None,
                method="GET", uri_stem="/", user_agent="UA", bytes_sent=100, bytes_recv=0),
        ])
        filtered = df.filter(col("client_ip").isNotNull() & (col("client_ip") != "-"))
        assert filtered.count() == 1

    def test_expect_valid_method(self, spark):
        """Rows with methods outside the allowed set are dropped (valid_method)."""
        from pyspark.sql.functions import col

        VALID_METHODS = ("GET", "POST", "HEAD", "PUT", "DELETE", "OPTIONS", "TRACE")
        df = spark.createDataFrame([
            Row(log_date=date(2009, 1, 1), status=200, client_ip="1.2.3.4",
                method="GET", uri_stem="/", user_agent="UA", bytes_sent=100, bytes_recv=0),
            Row(log_date=date(2009, 1, 1), status=200, client_ip="1.2.3.4",
                method="CONNECT", uri_stem="/", user_agent="UA", bytes_sent=100, bytes_recv=0),
            Row(log_date=date(2009, 1, 1), status=200, client_ip="1.2.3.4",
                method="PATCH", uri_stem="/", user_agent="UA", bytes_sent=100, bytes_recv=0),
            Row(log_date=date(2009, 1, 1), status=200, client_ip="1.2.3.4",
                method=None, uri_stem="/", user_agent="UA", bytes_sent=100, bytes_recv=0),
        ])
        filtered = df.filter(col("method").isin(VALID_METHODS))
        assert filtered.count() == 1

    def test_expect_valid_uri_stem(self, spark):
        """Rows with null uri_stem are dropped (valid_uri_stem)."""
        from pyspark.sql.functions import col

        df = spark.createDataFrame([
            Row(log_date=date(2009, 1, 1), status=200, client_ip="1.2.3.4",
                method="GET", uri_stem="/", user_agent="UA", bytes_sent=100, bytes_recv=0),
            Row(log_date=date(2009, 1, 1), status=200, client_ip="1.2.3.4",
                method="GET", uri_stem=None, user_agent="UA", bytes_sent=100, bytes_recv=0),
        ])
        filtered = df.filter(col("uri_stem").isNotNull())
        assert filtered.count() == 1

    def test_expect_valid_user_agent(self, spark):
        """Rows with null or '-' user_agent are dropped (valid_user_agent)."""
        from pyspark.sql.functions import col

        df = spark.createDataFrame([
            Row(log_date=date(2009, 1, 1), status=200, client_ip="1.2.3.4",
                method="GET", uri_stem="/", user_agent="Mozilla/5.0", bytes_sent=100, bytes_recv=0),
            Row(log_date=date(2009, 1, 1), status=200, client_ip="1.2.3.4",
                method="GET", uri_stem="/", user_agent="-", bytes_sent=100, bytes_recv=0),
            Row(log_date=date(2009, 1, 1), status=200, client_ip="1.2.3.4",
                method="GET", uri_stem="/", user_agent=None, bytes_sent=100, bytes_recv=0),
        ])
        filtered = df.filter(col("user_agent").isNotNull() & (col("user_agent") != "-"))
        assert filtered.count() == 1

    def test_expect_valid_bytes(self, spark):
        """Rows with negative bytes_sent or bytes_recv are dropped (valid_bytes)."""
        from pyspark.sql.functions import col

        df = spark.createDataFrame([
            Row(log_date=date(2009, 1, 1), status=200, client_ip="1.2.3.4",
                method="GET", uri_stem="/", user_agent="UA", bytes_sent=100, bytes_recv=50),
            Row(log_date=date(2009, 1, 1), status=200, client_ip="1.2.3.4",
                method="GET", uri_stem="/", user_agent="UA", bytes_sent=-1, bytes_recv=50),
            Row(log_date=date(2009, 1, 1), status=200, client_ip="1.2.3.4",
                method="GET", uri_stem="/", user_agent="UA", bytes_sent=100, bytes_recv=-5),
            Row(log_date=date(2009, 1, 1), status=200, client_ip="1.2.3.4",
                method="GET", uri_stem="/", user_agent="UA", bytes_sent=None, bytes_recv=50),
        ])
        # Expectation: (bytes_sent IS NULL OR bytes_sent >= 0) AND (bytes_recv IS NULL OR bytes_recv >= 0)
        filtered = df.filter(
            (col("bytes_sent").isNull() | (col("bytes_sent") >= 0)) &
            (col("bytes_recv").isNull() | (col("bytes_recv") >= 0))
        )
        assert filtered.count() == 2  # rows 1 and 4 pass


# ══════════════════════════════════════════════════════════════════════
#  11. Bronze table properties — structural checks
# ══════════════════════════════════════════════════════════════════════


class TestBronzeTableProperties:
    """Verify the Bronze table properties match documented configuration."""

    def test_partition_column_is_log_date(self):
        """The Bronze table must be partitioned by log_date."""
        from airflow.spark.databricks.dlt_bronze import bronze_raw_logs

        # Verify the DLT decorator includes partition_cols=["log_date"]
        # by inspecting the function's dlt metadata (available via __wrapped__)
        # Since dlt decorators add attributes at runtime, we verify the source
        # directly by checking the streaming_table decorator setup.
        import inspect
        source = inspect.getsource(bronze_raw_logs)
        assert "partition_cols" in source
        assert "log_date" in source

    def test_cdc_enabled(self):
        """Bronze must have ChangeDataFeed enabled for downstream Silver."""
        import inspect
        from airflow.spark.databricks.dlt_bronze import bronze_raw_logs

        source = inspect.getsource(bronze_raw_logs)
        assert "delta.enableChangeDataFeed" in source
        assert "true" in source

    def test_deletion_vectors_enabled(self):
        """Deletion vectors must be enabled for MERGE support."""
        import inspect
        from airflow.spark.databricks.dlt_bronze import bronze_raw_logs

        source = inspect.getsource(bronze_raw_logs)
        assert "delta.enableDeletionVectors" in source
        assert "true" in source
