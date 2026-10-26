"""
Bronze Ingestion — Databricks Unity Catalog Version
====================================================

Reads IIS W3C Extended Log Format files from DBFS, detects the per-file
column format (14 or 18 fields), parses each line into a structured Delta
table in Unity Catalog partitioned by ``log_date``. The job is
**incremental** — it tracks already-loaded files by their ``source_file``
value so re-runs only process new files.

This is the Databricks-equivalent of ``pipeline/spark/jobs/bronze_ingestion.py``.
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
from pyspark.sql.types import DateType, IntegerType, LongType, StringType, StructField, StructType

# ── Schema ──────────────────────────────────────────────────────────────
# Verbatim mirror of the bronze_schema from utils/schemas.py, self-contained
# so this notebook can run independently on Databricks (terraform provisions
# notebooks as single files — no utils/ bundle).

BRONZE_SCHEMA = StructType([
    StructField("log_date", DateType(), True),
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


# ── W3C Parser (verbatim mirror of utils/w3c_parser.py) ────────────────
# Inline parser so this script has no dependency on utils/ (terraform
# provisions notebooks as single files). SYNC OBLIGATION: any parser fix
# must be applied to BOTH this copy and pipeline/spark/jobs/utils/w3c_parser.py
# — keep the field parsing below byte-for-byte consistent with the reference.


def safe_int(val: str) -> int | None:
    """Convert to non-negative int or return None (mirrors w3c_parser.safe_int)."""
    try:
        v = int(val)
        return v if v >= 0 else None
    except (ValueError, TypeError):
        return None


def safe_date(val: str):
    """Parse an ISO ``YYYY-MM-DD`` date string, or return None (mirrors w3c_parser.safe_date)."""
    from datetime import datetime

    try:
        return datetime.strptime(val.strip(), "%Y-%m-%d").date()
    except (ValueError, TypeError, AttributeError):
        return None


def parse_log_line(line: str, file_format: int, source_file: str) -> dict | None:
    """Parse a single W3C log line into a dict matching BRONZE_SCHEMA.

    Field order follows the per-file ``#Fields:`` header, matching the
    reference parser in ``utils/w3c_parser.py``:

    * 14 fields: date time s-ip cs-method cs-uri-stem cs-uri-query s-port
      cs-username c-ip cs(User-Agent) sc-status sc-substatus
      sc-win32-status sc-time-taken
    * 18 fields: the above plus cs(Cookie) cs(Referer) after the UA, and
      sc-bytes cs-bytes before sc-time-taken.

    Returns ``None`` for malformed lines (skipped silently).
    """
    line = line.strip()
    try:
        if file_format == 14:
            # Last 4 tokens: status sub_status win32_status time_taken
            rs = line.rsplit(" ", 4)
            if len(rs) < 5:
                return None
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

        if file_format == 18:
            # Last 6 tokens: status sub_status win32_status bytes_sent bytes_recv time_taken
            rs = line.rsplit(" ", 6)
            if len(rs) < 7:
                return None
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

        return None
    except (IndexError, ValueError):
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
        SparkSession.builder
        .appName(app_name)
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
            log_date      DATE,
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
        display(spark.sql(f"SELECT COUNT(*) AS total_rows FROM {BRONZE_UC_PATH}"))  # type: ignore[name-defined]  # noqa: F821
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
