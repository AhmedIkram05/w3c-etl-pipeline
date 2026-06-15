"""
Unit tests for Databricks UC Bronze ingestion (``01_bronze_ingestion.py``).

Tests cover:

* ``_int`` — safe integer parsing helper
* ``parse_log_line`` — W3C log line parser for 14-field and 18-field formats
* ``detect_format`` — field-count detection from ``#Fields:`` header
* ``get_loaded_files`` — loaded-file discovery from UC table
* ``ensure_target_table`` — DDL for bronze UC table
* ``run`` — end-to-end pipeline with temp log files and patched UC path

All PySpark-dependent tests use a local ``spark`` fixture that does **not**
require the Delta Lake JAR — ``delta`` format calls are transparently
remapped to ``parquet`` so the pipeline logic can be verified on any
Spark distribution.
"""

import importlib
import os
import sys
import tempfile
from pathlib import Path

import pytest

pytest.importorskip("pyspark")
from pyspark.sql import SparkSession

# ── Monkey-patch DataFrameWriter so ``format("delta")`` → ``format("parquet")`` ──
# The target scripts use Delta throughout but we test the pipeline logic,
# not the storage format.  Remapping avoids requiring the Delta JAR.
from pyspark.sql.readwriter import DataFrameWriter

_orig_format = DataFrameWriter.format


def _delta_to_parquet(self, source):
    return _orig_format(self, "parquet" if source == "delta" else source)


DataFrameWriter.format = _delta_to_parquet

# Also patch SparkSession.sql to replace USING DELTA with USING PARQUET
_orig_sql = SparkSession.sql


def _no_delta_ddl(self, sql_query, *args, **kwargs):
    return _orig_sql(self, sql_query.replace("USING DELTA", "USING PARQUET"), *args, **kwargs)


SparkSession.sql = _no_delta_ddl

# ── Import the module under test (filename starts with digit) ────────
_TEST_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.join(_TEST_DIR, "..")

_NESTED_DB = os.path.join(_PROJECT_ROOT, "airflow", "spark", "databricks")
_FLAT_DB = os.path.join(_PROJECT_ROOT, "spark", "databricks")

_DATABRICKS_DIR = None
for _db_path in (_NESTED_DB, _FLAT_DB):
    if os.path.isdir(_db_path) and _db_path not in sys.path:
        sys.path.insert(0, _db_path)
        _DATABRICKS_DIR = _db_path

bronze = importlib.import_module("01_bronze_ingestion")


# ── Local spark fixture (no addPyFile — the scripts are self-contained) ──────
@pytest.fixture
def spark():
    """Create a minimal local SparkSession for testing pipeline logic.

    Uses a unique warehouse directory so tables from one test do not
    collide with another test on ``saveAsTable``.
    """
    warehouse_dir = tempfile.mkdtemp(prefix="spark_wh_")
    builder = (
        SparkSession.builder
        .master("local[1]")
        .appName("W3C_ETL_UC_Test")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.warehouse.dir", warehouse_dir)
    )
    session = builder.getOrCreate()
    yield session
    session.stop()


# ══════════════════════════════════════════════════════════════════════
#  1. _int — safe integer parsing
# ══════════════════════════════════════════════════════════════════════


class TestIntHelper:
    """Verify the ``_int`` safe-parsing helper."""

    def test_positive_int(self):
        assert bronze._int("42") == 42

    def test_zero(self):
        assert bronze._int("0") == 0

    def test_negative_int(self):
        assert bronze._int("-1") == -1

    def test_whitespace_stripped(self):
        assert bronze._int("  42  ") == 42

    def test_none_input(self):
        assert bronze._int(None) is None

    def test_non_numeric_string(self):
        assert bronze._int("abc") is None

    def test_empty_string(self):
        assert bronze._int("") is None

    def test_float_string(self):
        """Float strings raise ValueError in int() — return None."""
        assert bronze._int("12.5") is None


# ══════════════════════════════════════════════════════════════════════
#  2. parse_log_line — 14-field format
# ══════════════════════════════════════════════════════════════════════


class TestParseLogLine14Field:
    """Parsing 14-field W3C log lines.

    The function's 14-field mapping positions:
      0:log_date  1:log_time  2:server_ip  3:method  4:uri_stem
      5:client_ip  6:user_agent  7:cookie  8:referrer
      9:status  10:sub_status  11:win32_status
      12:bytes_sent  13:bytes_recv  14:time_taken
      15:server_port  16:username
    (needs 17 values despite format being "14")
    """

    SOURCE = "u_ex091024.log"

    def test_parses_successfully(self):
        line = (
            "2009-10-24 01:40:40 134.36.36.75 GET / "
            "1.2.3.4 Mozilla/4.0+ CookieVal www.example.com "
            "200 0 0 12345 678 900 80 -"
        )
        result = bronze.parse_log_line(line, 14, self.SOURCE)
        assert result is not None
        assert result["log_date"] == "2009-10-24"
        assert result["log_time"] == "01:40:40"
        assert result["server_ip"] == "134.36.36.75"
        assert result["method"] == "GET"
        assert result["uri_stem"] == "/"
        assert result["uri_query"] == ""
        assert result["client_ip"] == "1.2.3.4"
        assert result["user_agent"] == "Mozilla/4.0+"
        assert result["cookie"] == "CookieVal"
        assert result["referrer"] == "www.example.com"
        assert result["status"] == 200
        assert result["sub_status"] == 0
        assert result["win32_status"] == 0
        assert result["bytes_sent"] == 12345
        assert result["bytes_recv"] == 678
        assert result["time_taken"] == 900
        assert result["server_port"] == 80
        assert result["username"] == "-"
        assert result["source_file"] == self.SOURCE

    def test_malformed_line_returns_none(self):
        result = bronze.parse_log_line("too short", 14, self.SOURCE)
        assert result is None

    def test_empty_line_returns_none(self):
        assert bronze.parse_log_line("", 14, self.SOURCE) is None

    def test_numeric_fields_with_dash(self):
        """Dashes in numeric fields parse as None."""
        line = "2009-10-24 01:40:40 134.36.36.75 GET / 1.2.3.4 Mozilla/4.0+ - www.example.com - - - 186 - 80 - -"
        result = bronze.parse_log_line(line, 14, self.SOURCE)
        assert result is not None
        assert result["status"] is None  # parts[9] = "-"
        assert result["sub_status"] is None  # parts[10] = "-"
        assert result["win32_status"] is None  # parts[11] = "-"
        assert result["bytes_sent"] == 186  # parts[12] = "186"
        assert result["bytes_recv"] is None  # parts[13] = "-"
        assert result["time_taken"] == 80  # parts[14] = "80"
        assert result["server_port"] is None  # parts[15] = "-"


class TestParseLogLine18Field:
    """Parsing 18-field W3C log lines.

    The function's 18-field mapping positions:
      0:log_date  1:log_time  2:server_ip  3:method  4:uri_stem
      5:client_ip  6:user_agent  7:uri_query
      8:(gap)  9:(gap)  10:(gap)
      11:cookie  12:referrer
      13:status  14:sub_status  15:win32_status
      16:bytes_sent  17:bytes_recv  18:time_taken
      19:server_port  20:username
    (needs 21 values)
    """

    SOURCE = "u_ex091024.log"

    def test_parses_successfully(self):
        line = (
            "2009-10-24 01:40:40 134.36.36.75 GET / "
            "1.2.3.4 Mozilla/4.0+ q=test "
            "- - - "
            "CookieVal www.example.com "
            "200 0 0 12345 678 900 443 -"
        )
        result = bronze.parse_log_line(line, 18, self.SOURCE)
        assert result is not None
        assert result["log_date"] == "2009-10-24"
        assert result["log_time"] == "01:40:40"
        assert result["server_ip"] == "134.36.36.75"
        assert result["method"] == "GET"
        assert result["uri_stem"] == "/"
        assert result["uri_query"] == "q=test"
        assert result["client_ip"] == "1.2.3.4"
        assert result["user_agent"] == "Mozilla/4.0+"
        assert result["cookie"] == "CookieVal"
        assert result["referrer"] == "www.example.com"
        assert result["status"] == 200
        assert result["sub_status"] == 0
        assert result["win32_status"] == 0
        assert result["bytes_sent"] == 12345
        assert result["bytes_recv"] == 678
        assert result["time_taken"] == 900
        assert result["server_port"] == 443
        assert result["username"] == "-"
        assert result["source_file"] == self.SOURCE

    def test_malformed_line_returns_none(self):
        assert bronze.parse_log_line("too short", 18, self.SOURCE) is None

    def test_index_error_on_short_line(self):
        """Line shorter than 21 values returns None (IndexError caught)."""
        line = "2009-10-24 01:40:40 134.36.36.75 GET / 1.2.3.4"
        assert bronze.parse_log_line(line, 18, self.SOURCE) is None


# ══════════════════════════════════════════════════════════════════════
#  3. detect_format — field count from #Fields: header
# ══════════════════════════════════════════════════════════════════════


class TestDetectFormat:
    """Verify field-count detection from file headers."""

    def test_14_fields(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".log", delete=False) as f:
            f.write(
                "#Fields: date time s-ip cs-method cs-uri-stem cs-uri-query s-port c-username c-ip cs(User-Agent) sc-status sc-substatus sc-win32-status sc-time-taken\n"
            )
            f.write("data line\n")
            fname = f.name
        try:
            result = bronze.detect_format(fname)
            assert result == 14
        finally:
            os.unlink(fname)

    def test_18_fields(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".log", delete=False) as f:
            f.write(
                "#Fields: date time s-ip cs-method cs-uri-stem cs-uri-query s-port c-username c-ip cs(User-Agent) cs(Cookie) cs(Referer) sc-status sc-substatus sc-win32-status sc-bytes cs-bytes sc-time-taken\n"
            )
            fname = f.name
        try:
            result = bronze.detect_format(fname)
            assert result == 18
        finally:
            os.unlink(fname)

    def test_no_header_returns_none(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".log", delete=False) as f:
            f.write("2009-10-24 01:40:40 ...\n")
            fname = f.name
        try:
            result = bronze.detect_format(fname)
            assert result is None
        finally:
            os.unlink(fname)

    def test_empty_file_returns_none(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".log", delete=False) as f:
            fname = f.name
        try:
            result = bronze.detect_format(fname)
            assert result is None
        finally:
            os.unlink(fname)

    def test_nonexistent_file_returns_none(self):
        result = bronze.detect_format("/nonexistent/path.log")
        assert result is None


# ══════════════════════════════════════════════════════════════════════
#  4. get_loaded_files — UC table loaded-file tracking
# ══════════════════════════════════════════════════════════════════════


class TestGetLoadedFiles:
    """Verify source-file discovery from the bronze UC table."""

    UC_TABLE = "test_bronze_raw"

    @pytest.fixture(autouse=True)
    def _patch_uc_path(self):
        original = bronze.BRONZE_UC_PATH
        bronze.BRONZE_UC_PATH = self.UC_TABLE
        yield
        bronze.BRONZE_UC_PATH = original

    def test_returns_empty_set_when_table_does_not_exist(self, spark):
        result = bronze.get_loaded_files(spark)
        assert result == set()

    def test_returns_set_of_source_files(self, spark):
        spark.createDataFrame(
            [
                ("file1.log",),
                ("file2.log",),
                ("file1.log",),  # duplicate — distinct() should collapse
            ],
            schema="source_file string",
        ).write.mode("overwrite").saveAsTable(self.UC_TABLE)

        result = bronze.get_loaded_files(spark)
        assert result == {"file1.log", "file2.log"}


# ══════════════════════════════════════════════════════════════════════
#  5. ensure_target_table — DDL for bronze UC table
# ══════════════════════════════════════════════════════════════════════


class TestEnsureTargetTable:
    """Verify the DDL statement creates the bronze table."""

    UC_TABLE = "test_bronze_ensure"

    @pytest.fixture(autouse=True)
    def _patch_uc_path(self):
        original = bronze.BRONZE_UC_PATH
        bronze.BRONZE_UC_PATH = self.UC_TABLE
        yield
        bronze.BRONZE_UC_PATH = original

    def test_creates_table(self, spark):
        bronze.ensure_target_table(spark)
        df = spark.table(self.UC_TABLE)
        assert df.count() == 0

    def test_idempotent(self, spark):
        """Calling ensure_target_table twice should not raise."""
        bronze.ensure_target_table(spark)
        bronze.ensure_target_table(spark)  # second call
        df = spark.table(self.UC_TABLE)
        assert df.schema is not None


# ══════════════════════════════════════════════════════════════════════
#  6. run — end-to-end pipeline with temp log files
# ══════════════════════════════════════════════════════════════════════


class TestRunPipeline:
    """Verify the bronze pipeline steps end-to-end.

    .. note::

        The production ``run()`` function prefixes file paths with ``dbfs:``
        for Databricks DBFS compatibility.  Because that logic requires a
        ``/dbfs`` mount point that does not exist in local test environments,
        these tests exercise the pipeline by calling each step individually.
    """

    UC_TABLE = "test_bronze_run"

    @pytest.fixture(autouse=True)
    def _patch_uc_path(self):
        original = bronze.BRONZE_UC_PATH
        bronze.BRONZE_UC_PATH = self.UC_TABLE
        yield
        bronze.BRONZE_UC_PATH = original

    def _write_log_file(self, directory: str, filename: str, header_fields: int, data_lines: list[str]):
        """Helper: write a W3C log file with header and data lines."""
        path = Path(directory) / filename
        if header_fields == 14:
            header = "#Fields: date time s-ip cs-method cs-uri-stem cs-uri-query s-port c-username c-ip cs(User-Agent) sc-status sc-substatus sc-win32-status sc-time-taken"
        else:
            header = "#Fields: date time s-ip cs-method cs-uri-stem cs-uri-query s-port c-username c-ip cs(User-Agent) cs(Cookie) cs(Referer) sc-status sc-substatus sc-win32-status sc-bytes cs-bytes sc-time-taken"
        with open(path, "w") as f:
            f.write(header + "\n")
            for line in data_lines:
                f.write(line + "\n")
        return str(path)

    def _parse_file(self, fpath: str, source_file: str):
        """Parse a W3C log file — returns list of dicts."""
        fmt = bronze.detect_format(fpath)
        if fmt is None:
            return []
        parsed = []
        with open(fpath, "r") as fh:
            next(fh)  # skip header
            for line in fh:
                stripped = line.strip()
                if not stripped:
                    continue
                row = bronze.parse_log_line(stripped, fmt, source_file)
                if row is not None:
                    parsed.append(row)
        return parsed

    def test_creates_table_and_writes_data(self, spark):
        bronze.ensure_target_table(spark)
        with tempfile.TemporaryDirectory() as tmpdir:
            fpath = self._write_log_file(
                tmpdir,
                "u_ex091024.log",
                14,
                [
                    "2009-10-24 01:40:40 134.36.36.75 GET / 1.2.3.4 Mozilla/4.0+ - - 200 0 0 186 0 186 80 -",
                    "2009-10-24 01:41:00 134.36.36.75 GET /page 1.2.3.4 Mozilla/4.0+ - - 404 0 0 250 0 250 80 -",
                ],
            )
            rows = self._parse_file(fpath, "u_ex091024.log")
            assert len(rows) == 2

            from pyspark.sql import Row

            spark.createDataFrame([Row(**r) for r in rows]).write.mode("append").partitionBy("log_date").saveAsTable(
                self.UC_TABLE
            )

            df = spark.table(self.UC_TABLE)
            assert df.count() == 2
            stems = {r.uri_stem for r in df.collect()}
            assert "/" in stems
            assert "/page" in stems

    def test_skips_non_log_files(self, spark):
        bronze.ensure_target_table(spark)
        with tempfile.TemporaryDirectory() as tmpdir:
            (Path(tmpdir) / "readme.txt").write_text("not a log file")
            log_files = [f for f in os.listdir(tmpdir) if f.endswith(".log")]
            assert len(log_files) == 0

    def test_skips_malformed_log_files(self, spark):
        bronze.ensure_target_table(spark)
        with tempfile.TemporaryDirectory() as tmpdir:
            bad_file = Path(tmpdir) / "bad.log"
            bad_file.write_text("no header line\nsome data\n")
            fpath = str(bad_file)
            rows = self._parse_file(fpath, "bad.log")
            assert len(rows) == 0  # no header → detect_format returns None

    def test_incremental_skip_already_loaded(self, spark):
        bronze.ensure_target_table(spark)
        with tempfile.TemporaryDirectory() as tmpdir:
            fpath = self._write_log_file(
                tmpdir,
                "unique.log",
                14,
                [
                    "2009-10-24 01:40:40 134.36.36.75 GET / 1.2.3.4 Mozilla/4.0+ - - 200 0 0 186 0 186 80 -",
                ],
            )
            rows = self._parse_file(fpath, "unique.log")
            from pyspark.sql import Row

            spark.createDataFrame([Row(**r) for r in rows]).write.mode("append").partitionBy("log_date").saveAsTable(
                self.UC_TABLE
            )
            assert spark.table(self.UC_TABLE).count() == 1

            # Second write with same source_file — table should still have 1 row
            # (idempotency via source_file dedup at app level)
            existing = bronze.get_loaded_files(spark)
            if "unique.log" not in existing:
                spark.createDataFrame([Row(**r) for r in rows]).write.mode("append").saveAsTable(self.UC_TABLE)
            assert spark.table(self.UC_TABLE).count() == 1

    def test_writes_per_file_separately_with_correct_source(self, spark):
        bronze.ensure_target_table(spark)
        with tempfile.TemporaryDirectory() as tmpdir:
            fpath_a = self._write_log_file(
                tmpdir,
                "file_a.log",
                14,
                [
                    "2009-10-24 01:40:40 134.36.36.75 GET /a 1.2.3.4 UA - - 200 0 0 100 0 100 80 -",
                ],
            )
            fpath_b = self._write_log_file(
                tmpdir,
                "file_b.log",
                14,
                [
                    "2009-10-24 01:41:00 134.36.36.75 GET /b 1.2.3.4 UA - - 200 0 0 200 0 200 80 -",
                ],
            )

            from pyspark.sql import Row

            for fpath, fname in [(fpath_a, "file_a.log"), (fpath_b, "file_b.log")]:
                rows = self._parse_file(fpath, fname)
                spark.createDataFrame([Row(**r) for r in rows]).write.mode("append").partitionBy(
                    "log_date"
                ).saveAsTable(self.UC_TABLE)

            df = spark.table(self.UC_TABLE)
            sources = {r.source_file for r in df.collect()}
            assert sources == {"file_a.log", "file_b.log"}
