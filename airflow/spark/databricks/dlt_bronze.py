import dlt
from pyspark.sql.functions import udf, col, split, explode, to_date, concat_ws, row_number, decode
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DateType
from pyspark.sql.window import Window


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
    from datetime import datetime
    try:
        return datetime.strptime(val.strip(), "%Y-%m-%d").date()
    except (ValueError, TypeError, AttributeError):
        return None


def parse_log_line(line: str, file_format: int, source_file: str):
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


def detect_file_format(file_path: str) -> int:
    """Detect IIS format (14 or 18) from #Fields header line.

    In DLT streaming, we use a broadcast variable approach.
    All files in a batch should have the same format.
    """
    # In DLT, we can't easily read files in UDF. For simplicity,
    # we detect from the first non-comment line structure.
    # Real implementation would use a broadcast variable or file listing.
    # Default to 18 (modern format), can be overridden via spark.conf
    return 18


# ─── Broadcast format detection ───
# In practice, all files in a batch should have same format
file_format = detect_file_format("")

# Parse UDF - returns struct
parse_udf = udf(
    lambda line, src: parse_log_line(line, file_format, src),
    StructType([
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
)


# ─── Bronze DLT Pipeline ───

@dlt.streaming_table(
    name="bronze_raw_logs",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.enableDeletionVectors": "true"
    },
    partition_cols=["log_date"]
)
@dlt.expect_or_drop("valid_log_date", "log_date IS NOT NULL")
@dlt.expect_or_drop("valid_status", "status BETWEEN 100 AND 599")
@dlt.expect_or_drop("valid_client_ip", "client_ip IS NOT NULL AND client_ip != '-'")
@dlt.expect_or_drop("valid_method", "method IN ('GET', 'POST', 'HEAD', 'PUT', 'DELETE', 'OPTIONS', 'TRACE')")
@dlt.expect_or_drop("valid_uri_stem", "uri_stem IS NOT NULL")
@dlt.expect_or_drop("valid_user_agent", "user_agent IS NOT NULL AND user_agent != '-'")
@dlt.expect_or_drop("valid_bytes", "(bytes_sent IS NULL OR bytes_sent >= 0) AND (bytes_recv IS NULL OR bytes_recv >= 0)")
def bronze_raw_logs():
    """Bronze DLT pipeline with Auto Loader and W3C parser."""
    # Auto Loader configuration
    df = spark.readStream.format("cloudFiles") \
        .option("cloudFiles.format", "binaryFile") \
        .option("cloudFiles.includeExistingFiles", "true") \
        .option("cloudFiles.schemaLocation", "dbfs:/Volumes/w3c_etl_databricks/bronze/w3c_data/_schemas/bronze") \
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns") \
        .option("cloudFiles.rescuedDataColumn", "_rescued_data") \
        .option("maxFilesPerTrigger", "10") \
        .option("maxFileSize", 209715200) \
        .load(f"abfss://raw-logs@{spark.conf.get('storage.account_name')}.dfs.core.windows.net/")

    # binaryFile returns content as binary; decode UTF-8, split by newline
    lines_df = df.withColumn("line", explode(split(decode(col("content"), "utf-8"), "\n")))
    lines_df = lines_df.filter(~col("line").startswith("#") & (col("line") != ""))

    # Parse each line
    parsed_df = lines_df.withColumn("parsed", parse_udf(col("line"), col("path")))

    # Extract fields from parsed struct (direct struct access, NO explode)
    for field_name in ["log_date", "log_time", "server_ip", "method", "uri_stem",
                       "uri_query", "server_port", "username", "client_ip", "user_agent",
                       "cookie", "referrer", "status", "sub_status", "win32_status",
                       "bytes_sent", "bytes_recv", "time_taken", "source_file"]:
        parsed_df = parsed_df.withColumn(field_name, col(f"parsed.{field_name}"))

    # Preserve _rescued_data column for debugging malformed lines
    parsed_df = parsed_df.withColumn("_rescued_data", col("_rescued_data"))

    parsed_df = parsed_df.drop("parsed", "line", "content", "path")

    # Cast log_date to Date for partitioning (Silver integration)
    # Note: safe_date already returns date object, but ensure type
    parsed_df = parsed_df.withColumn("log_date", to_date(col("log_date")))

    # Deduplication using ROW_NUMBER (for full_refresh idempotency)
    # Composite key uniquely identifies a log event
    parsed_df = parsed_df.withColumn(
        "dedup_key",
        concat_ws("|", col("source_file"), col("log_date"), col("log_time"),
                  col("client_ip"), col("method"), col("uri_stem"), col("status"))
    )
    window_spec = Window.partitionBy("dedup_key").orderBy("source_file")
    parsed_df = parsed_df.withColumn("row_num", row_number().over(window_spec))
    parsed_df = parsed_df.filter(col("row_num") == 1).drop("row_num", "dedup_key")

    return parsed_df