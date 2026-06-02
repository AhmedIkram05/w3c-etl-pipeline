"""
Bronze Ingestion — W3C Medallion Architecture
==============================================

Reads IIS W3C Extended Log Format files, detects the per-file column
format (14 or 18 fields), parses each line into a structured Delta table
partitioned by ``log_date``. The job is **incremental** — it tracks
already-loaded files by their ``source_file`` value so re-runs only
process new files.

Usage
-----
::

    spark-submit bronze_ingestion.py \\
        --log-files-dir /opt/spark/data/LogFiles \\
        --delta-dir /opt/spark/delta

    # Or via Makefile:
    make spark-run-bronze
"""

import argparse
import os
import sys

from pyspark.sql import SparkSession

# The ``utils`` package is bundled as ``--py-files utils.zip`` for Spark executors.
# When running directly (e.g. ``python3 bronze_ingestion.py``), the local path
# is needed since ``spark-submit`` is not involved.
try:
    from utils.schemas import bronze_schema
    from utils.w3c_parser import parse_log_line
except ImportError:
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "utils"))
    from schemas import bronze_schema
    from w3c_parser import parse_log_line


# ── Helpers ─────────────────────────────────────────────────────────


def create_spark_session(app_name: str = "w3c_bronze") -> SparkSession:
    """Create a Spark session.

    Delta Lake configuration is loaded from
    ``$SPARK_HOME/conf/spark-defaults.conf`` which is mounted as a
    read-only volume by Docker Compose.
    """
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )


def get_loaded_files(spark, bronze_path: str) -> set:
    """Return the set of ``source_file`` values already in the bronze table."""
    try:
        existing = spark.read.format("delta").load(bronze_path)
        return {row.source_file for row in existing.select("source_file").distinct().collect()}
    except Exception:
        return set()


def detect_format(file_path: str):
    """Read the ``#Fields:`` header line and return the field count.

    Returns ``None`` if no header is found, or the field count (14 or 18)
    otherwise. Unrecognised counts are returned as-is for the caller to
    handle.
    """
    try:
        with open(file_path, "r", errors="replace") as fh:
            for line in fh:
                line = line.rstrip("\n")
                if line.startswith("#Fields:"):
                    fields = line.replace("#Fields:", "").strip().split()
                    return len(fields)
    except Exception as exc:
        print(f"    ERROR reading header: {exc}")
    return None


def parse_file_to_df(spark, file_path: str, file_format: int, source_file: str):
    """Read a ``.log`` file and return a DataFrame with ``bronze_schema``.

    Comment lines (``#``) and empty lines are silently skipped. Lines
    that fail to parse are also skipped.
    """
    lines_rdd = spark.sparkContext.textFile(file_path)

    # Filter out empty lines and comment lines
    data_lines = lines_rdd.filter(lambda line: bool(line) and not line.startswith("#"))

    # Parse each remaining line
    parsed_rdd = data_lines.map(lambda line: parse_log_line(line, file_format, source_file)).filter(
        lambda r: r is not None
    )

    if parsed_rdd.isEmpty():
        return None

    return spark.createDataFrame(parsed_rdd, schema=bronze_schema)


# ── Main ────────────────────────────────────────────────────────────


def run(spark, log_files_dir: str, delta_dir: str) -> int:
    """Discover new ``.log`` files, parse them, and write to bronze Delta."""
    bronze_path = os.path.join(delta_dir, "bronze")

    # --- Determine already-loaded files -------------------------------
    loaded_files = get_loaded_files(spark, bronze_path)
    print(f"[Bronze] Already loaded: {len(loaded_files)} file(s)")

    # --- Discover new files -------------------------------------------
    all_files = sorted(f for f in os.listdir(log_files_dir) if f.endswith(".log"))
    new_files = [f for f in all_files if f not in loaded_files]

    if not new_files:
        print("[Bronze] No new log files to process.")
        return 0

    print(f"[Bronze] Found {len(new_files)} new file(s): {new_files}")

    # --- Process each file --------------------------------------------
    total_rows = 0

    for fname in new_files:
        fpath = os.path.join(log_files_dir, fname)
        print(f"  {fname} ...", end=" ", flush=True)

        # 1. Detect format
        file_format = detect_format(fpath)
        if file_format is None:
            print("SKIPPED (no #Fields header)")
            continue
        if file_format not in (14, 18):
            print(f"SKIPPED (unrecognised field count {file_format})")
            continue

        # 2. Parse
        df = parse_file_to_df(spark, fpath, file_format, fname)
        if df is None:
            print("SKIPPED (no data rows)")
            continue

        row_count = df.count()
        print(f"{row_count} rows", end="", flush=True)

        # 3. Write to Delta (partitioned by log_date)
        df.write.format("delta").mode("append").partitionBy("log_date").option("path", bronze_path).save()

        total_rows += row_count
        print(f"  (cumulative: {total_rows})")

    print(f"[Bronze] Complete. {total_rows} rows across {len(new_files)} file(s).")
    return total_rows


def main():
    parser = argparse.ArgumentParser(description="W3C Bronze Ingestion")
    parser.add_argument(
        "--log-files-dir",
        default="/opt/spark/data/LogFiles",
        help="Directory containing .log files (default: %(default)s)",
    )
    parser.add_argument(
        "--delta-dir",
        default="/opt/spark/delta",
        help="Root directory for Delta Lake tables (default: %(default)s)",
    )
    args = parser.parse_args()

    spark = create_spark_session()
    try:
        run(spark, args.log_files_dir, args.delta_dir)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
