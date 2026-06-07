"""
Bronze Ingestion — Databricks Unity Catalog Version
====================================================

Reads IIS W3C Extended Log Format files from DBFS, detects the per-file
column format (14 or 18 fields), parses each line into a structured Delta
table in Unity Catalog partitioned by ``log_date``. The job is
**incremental** — it tracks already-loaded files by their ``source_file``
value so re-runs only process new files.

This is the Databricks-equivalent of ``airflow/spark/jobs/bronze_ingestion.py``.
It uses Unity Catalog paths instead of local Delta directories.

Usage
-----
Run as a Databricks Python notebook or script on a cluster with
access to the ``w3c_catalog`` Unity Catalog namespace::

    # Via Databricks CLI or workflow
    databricks jobs run-now --job-id <job-id> \\
        --jar-params '["--log-files-dir","dbfs:/mnt/w3c-logs/LogFiles"]'

Requirements
------------
- Unity Catalog: ``w3c_catalog.bronze.raw_logs`` target table must exist
  or be created by the first run
- Source files in DBFS or external location mounted at ``dbfs:/mnt/w3c-logs/``
- Cluster configured with Unity Catalog + Delta Lake support
"""

import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType

# ── Schema ──────────────────────────────────────────────────────────────
# Replicates the bronze_schema from utils/schemas.py but self-contained
# so this notebook can run independently on Databricks.

BRONZE_SCHEMA = StructType([
    StructField("log_date", StringType(), True),
    StructField("log_time", StringType(), True),
    StructField("server_ip", StringType(), True),
    StructField("method", StringType(), True),
    StructField("uri_stem", StringType(), True),
    StructField("uri_query", StringType(), True),
    StructField("client_ip", StringType(), True),
    StructField("user_agent", StringType(), True),
    StructField("cookie", StringType(), True),
    StructField("referrer", StringType(), True),
    StructField("status", IntegerType(), True),
    StructField("sub_status", IntegerType(), True),
    StructField("win32_status", IntegerType(), True),
    StructField("bytes_sent", LongType(), True),
    StructField("bytes_recv", LongType(), True),
    StructField("server_port", IntegerType(), True),
    StructField("username", StringType(), True),
    StructField("time_taken", LongType(), True),
    StructField("source_file", StringType(), True),
])

# ── Unity Catalog target ───────────────────────────────────────────────
# Configurable — update CATALOG, SCHEMA, TABLE for your workspace.
CATALOG = "w3c_catalog"
SCHEMA = "bronze"
TABLE = "raw_logs"

BRONZE_UC_PATH = f"{CATALOG}.{SCHEMA}.{TABLE}"


# ── W3C Parser (self-contained) ────────────────────────────────────────
# Inline minimal parser so this script has no dependency on utils/.


def parse_log_line(line: str, file_format: int, source_file: str) -> dict | None:
    """Parse a single W3C log line into a dict matching BRONZE_SCHEMA.

    Returns ``None`` for malformed lines (skipped silently).
    """
    parts = line.strip().split()
    if len(parts) < file_format:
        return None

    try:
        row = {
            "log_date": parts[0],
            "log_time": parts[1],
            "server_ip": parts[2],
            "method": parts[3],
            "uri_stem": parts[4],
            "client_ip": parts[5],
            "user_agent": parts[6] if len(parts) > 6 else "",
            "cookie": parts[11] if file_format == 18 else parts[7],
            "referrer": parts[12] if file_format == 18 else parts[8],
            "status": _int(parts[13] if file_format == 18 else parts[9]),
            "sub_status": _int(parts[14] if file_format == 18 else parts[10]),
            "win32_status": _int(parts[15] if file_format == 18 else parts[11]),
            "bytes_sent": _int(parts[16] if file_format == 18 else parts[12]),
            "bytes_recv": _int(parts[17] if file_format == 18 else parts[13]),
            "time_taken": _int(parts[18] if file_format == 18 else parts[14]),
            "server_port": _int(parts[19] if file_format == 18 else parts[15]),
            "username": parts[20] if file_format == 18 else parts[16],
            "uri_query": parts[7] if file_format == 18 else "",
            "source_file": source_file,
        }
    except (IndexError, ValueError):
        return None

    return row


def _int(val: str) -> int | None:
    """Safely parse an integer, returning None for malformed values."""
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def detect_format(file_path: str) -> int | None:
    """Read the ``#Fields:`` header line and return the field count."""
    try:
        # Databricks: read from DBFS using local file API
        with open(file_path.replace("dbfs:", "/dbfs"), "r", errors="replace") as fh:
            for line in fh:
                line = line.rstrip("\n")
                if line.startswith("#Fields:"):
                    fields = line.replace("#Fields:", "").strip().split()
                    return len(fields)
    except Exception as exc:
        print(f"    ERROR reading header: {exc}")
    return None


# ── Helpers ─────────────────────────────────────────────────────────────


def create_spark_session(app_name: str = "databricks_bronze") -> SparkSession:
    """Create or reuse a Spark session for Databricks."""
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )


def get_loaded_files(spark) -> set:
    """Return ``source_file`` values already in the Unity Catalog bronze table."""
    try:
        existing = spark.table(BRONZE_UC_PATH)
        return {row.source_file for row in existing.select("source_file").distinct().collect()}
    except Exception:
        return set()


def ensure_target_table(spark):
    """Create the Unity Catalog bronze table if it does not exist.

    Uses ``CREATE TABLE IF NOT EXISTS`` with Delta format and partitioning
    by ``log_date``.
    """
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {BRONZE_UC_PATH} (
            log_date      STRING,
            log_time      STRING,
            server_ip     STRING,
            method        STRING,
            uri_stem      STRING,
            uri_query     STRING,
            client_ip     STRING,
            user_agent    STRING,
            cookie        STRING,
            referrer      STRING,
            status        INT,
            sub_status    INT,
            win32_status  INT,
            bytes_sent    BIGINT,
            bytes_recv    BIGINT,
            server_port   INT,
            username      STRING,
            time_taken    BIGINT,
            source_file   STRING
        )
        USING DELTA
        PARTITIONED BY (log_date)
        """
    )
    print(f"Ensured target table {BRONZE_UC_PATH} exists.")


# ── Main ────────────────────────────────────────────────────────────────


def run(spark, log_files_dir: str):
    """Discover new .log files, parse them, and write to bronze UC table."""
    # Ensure the Unity Catalog table exists
    ensure_target_table(spark)

    # Determine already-loaded files
    loaded_files = get_loaded_files(spark)
    print(f"[Bronze-DB] Already loaded: {len(loaded_files)} file(s)")

    # Normalise path: if dbfs:/mnt/... convert to /dbfs/mnt/... for os.listdir
    fs_path = log_files_dir.replace("dbfs:", "/dbfs") if log_files_dir.startswith("dbfs:") else log_files_dir

    # Discover new files
    all_files = sorted(f for f in os.listdir(fs_path) if f.endswith(".log"))
    new_files = [f for f in all_files if f not in loaded_files]

    if not new_files:
        print("[Bronze-DB] No new log files to process.")
        return

    print(f"[Bronze-DB] Found {len(new_files)} new file(s): {new_files}")

    total_rows = 0

    for fname in new_files:
        fpath = os.path.join(fs_path, fname)
        print(f"  {fname} ...", end=" ", flush=True)

        # Detect format
        file_format = detect_format(f"dbfs:{fpath}" if not fpath.startswith("/dbfs") else fpath)
        if file_format is None:
            print("SKIPPED (no #Fields header)")
            continue
        if file_format not in (14, 18):
            print(f"SKIPPED (unrecognised field count {file_format})")
            continue

        # Parse
        lines_rdd = spark.sparkContext.textFile(f"file://{fpath}")
        data_lines = lines_rdd.filter(lambda line: bool(line) and not line.startswith("#"))
        parsed_rdd = data_lines.map(lambda line: parse_log_line(line, file_format, fname)).filter(
            lambda r: r is not None
        )

        if parsed_rdd.isEmpty():
            print("SKIPPED (no data rows)")
            continue

        df = spark.createDataFrame(parsed_rdd, schema=BRONZE_SCHEMA)
        row_count = df.count()
        print(f"{row_count} rows", end="", flush=True)

        # Write to Unity Catalog Delta table
        df.write.format("delta").mode("append").partitionBy("log_date").saveAsTable(BRONZE_UC_PATH)

        total_rows += row_count
        print(f"  (cumulative: {total_rows})")

    print(f"[Bronze-DB] Complete. {total_rows} rows across {len(new_files)} file(s).")

    # Display summary (Databricks notebook output)
    try:
        display(spark.sql(f"SELECT COUNT(*) AS total_rows FROM {BRONZE_UC_PATH}"))
    except NameError:
        pass  # display() only available in Databricks notebooks


def main():
    parser = argparse.ArgumentParser(description="W3C Bronze Ingestion (Databricks UC)")
    parser.add_argument(
        "--log-files-dir",
        default="dbfs:/mnt/w3c-logs/LogFiles",
        help="Directory containing .log files on DBFS (default: %(default)s)",
    )
    args = parser.parse_args()

    spark = create_spark_session()
    try:
        run(spark, args.log_files_dir)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
