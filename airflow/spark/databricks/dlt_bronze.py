from datetime import datetime

import dlt
from pyspark.sql.functions import col, explode, to_date, udf
from pyspark.sql.types import (
    ArrayType,
    DateType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# ─── W3C Parser (from authoritative w3c_parser.py) ───


def safe_int(val: str):
    """Convert to non-negative int or return None."""
    try:
        v = int(val)
        return v if v >= 0 else None
    except (ValueError, TypeError):
        return None


def safe_date(val: str):
    """Parse an ISO YYYY-MM-DD date string, or return None."""
    try:
        return datetime.strptime(val.strip(), "%Y-%m-%d").date()
    except (ValueError, TypeError, AttributeError):
        return None


def _parse_log_line(line: str, file_format: int, source_file: str):
    """Parse a single W3C log line into a dict matching bronze schema.

    Uses rsplit field-counting to handle unquoted user-agent strings.
    Supports both 14-field and 18-field IIS formats.
    """
    try:
        if file_format == 14:
            # 14-field format: last 4 tokens are fixed numeric fields
            rs = line.rsplit(" ", 4)
            if len(rs) < 5:
                return None
            # First 10 tokens are the variable-width prefix
            ls = rs[0].split(" ", 9)
            if len(ls) < 10:
                return None

            return {
                "log_date": safe_date(ls[0]),
                "log_time": ls[1],
                "server_ip": ls[2],
                "method": ls[3],
                "uri_stem": ls[4],
                "uri_query": ls[5],
                "server_port": safe_int(ls[6]),
                "username": ls[7],
                "client_ip": ls[8],
                "user_agent": ls[9],
                "cookie": None,
                "referrer": None,
                "status": safe_int(rs[1]),
                "sub_status": safe_int(rs[2]),
                "win32_status": safe_int(rs[3]),
                "bytes_sent": None,
                "bytes_recv": None,
                "time_taken": safe_int(rs[4]),
                "source_file": source_file,
            }

        elif file_format == 18:
            # 18-field format: last 6 tokens are fixed numeric fields
            rs = line.rsplit(" ", 6)
            if len(rs) < 7:
                return None
            # First 12 tokens include cookie and referrer
            ls = rs[0].split(" ", 11)
            if len(ls) < 12:
                return None

            referrer = ls[11].strip()
            return {
                "log_date": safe_date(ls[0]),
                "log_time": ls[1],
                "server_ip": ls[2],
                "method": ls[3],
                "uri_stem": ls[4],
                "uri_query": ls[5],
                "server_port": safe_int(ls[6]),
                "username": ls[7],
                "client_ip": ls[8],
                "user_agent": ls[9],
                "cookie": ls[10],
                "referrer": referrer,
                "status": safe_int(rs[1]),
                "sub_status": safe_int(rs[2]),
                "win32_status": safe_int(rs[3]),
                "bytes_sent": safe_int(rs[4]),
                "bytes_recv": safe_int(rs[5]),
                "time_taken": safe_int(rs[6]),
                "source_file": source_file,
            }

    except Exception:
        return None

    return None


def _detect_format_from_content(content_bytes: bytes) -> int:
    """Detect IIS format (14 or 18) by reading #Fields: header from file content.

    Counts field names in the header line to determine whether this is
    a 14-field (2009-mid 2010) or 18-field (mid 2010+) IIS W3C log.
    Defaults to 18 if no header is found.
    """
    try:
        text = content_bytes.decode("utf-8", errors="replace")
        for line in text.split("\n"):
            if line.startswith("#Fields:"):
                fields = line.replace("#Fields:", "").strip().split()
                return 14 if len(fields) == 14 else 18
    except Exception:
        pass
    return 18


def _parse_file_content(content_bytes: bytes, source_path: str) -> list:
    """Parse entire W3C log file content into a list of row dicts.

    Detects the file format (14 or 18 fields) from the ``#Fields:``
    header, then parses every non-comment data line.  Returns a list
    of dicts matching ``_PARSED_ROW_SCHEMA``.

    This runs inside a UDF on each worker — one call per file row in
    the binaryFile stream.
    """
    try:
        text = content_bytes.decode("utf-8", errors="replace")
        lines = text.split("\n")

        # Detect format from #Fields: header (first matching line)
        file_format = 18
        for line in lines:
            if line.startswith("#Fields:"):
                fields = line.replace("#Fields:", "").strip().split()
                file_format = 14 if len(fields) == 14 else 18
                break

        # Parse every non-comment, non-empty line
        results = []
        for line in lines:
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue
            result = _parse_log_line(stripped, file_format, source_path)
            if result is not None:
                results.append(result)

        return results
    except Exception:
        return []


# Schema for a single parsed log row
_PARSED_ROW_SCHEMA = StructType([
    StructField("log_date", DateType(), True),
    StructField("log_time", StringType(), True),
    StructField("server_ip", StringType(), True),
    StructField("method", StringType(), True),
    StructField("uri_stem", StringType(), True),
    StructField("uri_query", StringType(), True),
    StructField("server_port", IntegerType(), True),
    StructField("username", StringType(), True),
    StructField("client_ip", StringType(), True),
    StructField("user_agent", StringType(), True),
    StructField("cookie", StringType(), True),
    StructField("referrer", StringType(), True),
    StructField("status", IntegerType(), True),
    StructField("sub_status", IntegerType(), True),
    StructField("win32_status", IntegerType(), True),
    StructField("bytes_sent", LongType(), True),
    StructField("bytes_recv", LongType(), True),
    StructField("time_taken", LongType(), True),
    StructField("source_file", StringType(), True),
])

# UDF: binary file content -> array of parsed row structs
# Each call processes one complete file, detects format from #Fields: header,
# and returns all parsed rows.  This replaces the old per-line UDF pattern
# that was closed over a stale module-level file_format variable.
parse_file_udf = udf(
    _parse_file_content,
    ArrayType(_PARSED_ROW_SCHEMA),
)


# ─── Bronze DLT Pipeline ───


@dlt.table(
    name="bronze_raw_logs",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.enableDeletionVectors": "true",
    },
    partition_cols=["log_date"],
)
@dlt.expect_or_drop("valid_log_date", "log_date IS NOT NULL")
@dlt.expect_or_drop("valid_status", "status BETWEEN 100 AND 599")
@dlt.expect_or_drop("valid_client_ip", "client_ip IS NOT NULL AND client_ip != '-'")
@dlt.expect_or_drop("valid_method", "method IN ('GET', 'POST', 'HEAD', 'PUT', 'DELETE', 'OPTIONS', 'TRACE')")
@dlt.expect_or_drop("valid_uri_stem", "uri_stem IS NOT NULL")
@dlt.expect_or_drop("valid_user_agent", "user_agent IS NOT NULL AND user_agent != '-'")
@dlt.expect_or_drop(
    "valid_bytes", "(bytes_sent IS NULL OR bytes_sent >= 0) AND (bytes_recv IS NULL OR bytes_recv >= 0)"
)
def bronze_raw_logs():
    """Bronze DLT pipeline with Auto Loader and W3C parser.

    Uses a per-file UDF pattern (not per-line) so that each file's
    ``#Fields:`` header is read to detect 14-field vs 18-field format.
    This fixes CRIT-01 (hardcoded format) and CRIT-02 (UDF closure
    over a stale module variable).
    """
    # Validate required config before reading
    storage_account = spark.conf.get("storage.account_name")  # noqa: F821
    if not storage_account:
        raise ValueError(
            "Missing required Spark config: 'storage.account_name'. Set via pipeline configuration or spark.conf.set()."
        )

    # Auto Loader configuration (binaryFile -> per-file rows)
    # Authorization is provided via Unity Catalog external location with managed identity
    df = (
        spark.readStream  # noqa: F821
        .format("cloudFiles")
        .option("cloudFiles.format", "binaryFile")
        .option("cloudFiles.includeExistingFiles", "true")
        .option("cloudFiles.schemaLocation", "dbfs:/Volumes/w3c_etl_databricks/bronze/w3c_data/_schemas/bronze")
        .option("cloudFiles.schemaEvolutionMode", "none")
        .option("cloudFiles.rescuedDataColumn", "_rescued_data")
        .option("maxFilesPerTrigger", "10")
        .option("maxFileSize", 209715200)
        .load(f"abfss://raw-logs@{storage_account}.dfs.core.windows.net/")
    )

    # Parse entire file content per file: detect format from #Fields: header,
    # parse all lines, return array of structs — then explode to rows.
    # The UDF is self-contained and detects format per-file, fixing both
    parsed_df = df.withColumn("parsed_rows", parse_file_udf(col("content"), col("path")))

    # Explode array of structs into individual rows
    parsed_df = parsed_df.select(explode(col("parsed_rows")).alias("parsed"))

    # Extract fields from parsed struct (direct struct access, no explode)
    for field_name in [
        "log_date",
        "log_time",
        "server_ip",
        "method",
        "uri_stem",
        "uri_query",
        "server_port",
        "username",
        "client_ip",
        "user_agent",
        "cookie",
        "referrer",
        "status",
        "sub_status",
        "win32_status",
        "bytes_sent",
        "bytes_recv",
        "time_taken",
        "source_file",
    ]:
        parsed_df = parsed_df.withColumn(field_name, col(f"parsed.{field_name}"))

    parsed_df = parsed_df.drop("parsed")

    # Cast log_date to Date for partitioning (Silver integration)
    # safe_date already returns a date, but ensure Spark type
    parsed_df = parsed_df.withColumn("log_date", to_date(col("log_date")))

    return parsed_df
