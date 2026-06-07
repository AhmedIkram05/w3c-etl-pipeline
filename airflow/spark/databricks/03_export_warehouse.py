"""
Export Warehouse — Databricks Unity Catalog Version
====================================================

Reads the Silver Delta table from Unity Catalog and writes it to a Gold
analytics table in Unity Catalog for BI consumption. This is the
Databricks-equivalent of ``airflow/spark/jobs/export_warehouse.py``.

Unlike the Docker-based version which uses Spark JDBC to write to
PostgreSQL, the Databricks version writes directly to a Unity Catalog
Gold table. This serves as the analytics-ready dataset for downstream
consumers (Power BI, dbt, or direct SQL queries).

Usage
-----
Run as a Databricks Python notebook or script on a cluster with access to
the ``w3c_catalog`` Unity Catalog namespace::

    # Via Databricks CLI or workflow
    databricks jobs run-now --job-id <job-id>

Requirements
------------
- Unity Catalog tables: ``w3c_catalog.silver.enriched_logs`` and
  ``w3c_catalog.gold.warehouse_enriched``
- Cluster configured with Unity Catalog + Delta Lake support

Idempotency
-----------
The job uses a tracking table (``w3c_catalog.gold.export_tracking``)
to record which ``source_file`` values have already been exported.
Only new source files are written on each run. The gold table is
appended to incrementally.
"""

import argparse
import logging
import sys
from typing import Set

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("databricks_export")

# ── Unity Catalog paths ─────────────────────────────────────────────────
# Configurable — update for your workspace.
CATALOG = "w3c_catalog"
SILVER_SCHEMA = "silver"
SILVER_TABLE = "enriched_logs"
GOLD_SCHEMA = "gold"
GOLD_TABLE = "warehouse_enriched"
TRACKING_TABLE = "export_tracking"

SILVER_UC_PATH = f"{CATALOG}.{SILVER_SCHEMA}.{SILVER_TABLE}"
GOLD_UC_PATH = f"{CATALOG}.{GOLD_SCHEMA}.{GOLD_TABLE}"
TRACKING_UC_PATH = f"{CATALOG}.{GOLD_SCHEMA}.{TRACKING_TABLE}"


# ── Helpers ─────────────────────────────────────────────────────────────


def create_spark_session(app_name: str = "databricks_export") -> SparkSession:
    """Create or reuse a Spark session for Databricks."""
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )


def get_source_files_in_table(spark, uc_path: str) -> Set[str]:
    """Return ``source_file`` values already in a UC table.

    Returns an empty set if the table does not exist yet.
    """
    try:
        df = spark.table(uc_path)
        rows = df.select("source_file").distinct().collect()
        return {row.source_file for row in rows}
    except Exception:
        return set()


def ensure_gold_tables(spark):
    """Create the Unity Catalog gold tables if they do not exist.

    - ``gold.warehouse_enriched``: 35-column enriched dataset
    - ``gold.export_tracking``: idempotency tracking table
    """
    # Main gold table (mirrors silver schema)
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {GOLD_UC_PATH} (
            log_date         STRING,
            log_time         STRING,
            server_ip        STRING,
            method           STRING,
            uri_stem         STRING,
            uri_query        STRING,
            client_ip        STRING,
            user_agent       STRING,
            cookie           STRING,
            referrer         STRING,
            status           INT,
            sub_status       INT,
            win32_status     INT,
            bytes_sent       BIGINT,
            bytes_recv       BIGINT,
            server_port      INT,
            username         STRING,
            time_taken       BIGINT,
            source_file      STRING,

            page_category    STRING,
            referrer_domain  STRING,
            traffic_type     STRING,
            is_crawler       STRING,
            size_band        STRING
        )
        USING DELTA
        PARTITIONED BY (log_date)
        """
    )
    log.info("Ensured gold table %s exists.", GOLD_UC_PATH)

    # Tracking table for idempotency
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {TRACKING_UC_PATH} (
            source_file STRING
        )
        USING DELTA
        """
    )
    log.info("Ensured tracking table %s exists.", TRACKING_UC_PATH)


def insert_tracking_records(spark, new_source_files: Set[str]):
    """Insert new ``source_file`` values into the tracking table."""
    if not new_source_files:
        return

    from pyspark.sql.types import StringType, StructField, StructType

    tracking_schema = StructType([StructField("source_file", StringType(), True)])
    rows = [[sf] for sf in new_source_files]
    tracking_df = spark.createDataFrame(rows, schema=tracking_schema)

    tracking_df.write.format("delta").mode("append").saveAsTable(TRACKING_UC_PATH)
    log.info(
        "Inserted %d new source_file(s) into %s.",
        len(new_source_files),
        TRACKING_UC_PATH,
    )


# ── Main pipeline ───────────────────────────────────────────────────────


def run(spark):
    """Execute the Silver → Gold export pipeline on Databricks."""
    # Ensure target tables exist
    ensure_gold_tables(spark)

    # Read Silver table
    try:
        silver_df = spark.table(SILVER_UC_PATH)
    except Exception as exc:
        log.error("Cannot read Silver table %s: %s", SILVER_UC_PATH, exc)
        return

    total_rows = silver_df.count()
    if total_rows == 0:
        log.warning("Silver table is empty. Nothing to export.")
        return

    log.info("Loaded Silver with %d rows.", total_rows)

    # Determine new source_files (not yet in tracking table)
    tracked_files = get_source_files_in_table(spark, TRACKING_UC_PATH)
    log.info("Already tracked source_files: %d.", len(tracked_files))

    all_sources = set(row.source_file for row in silver_df.select("source_file").distinct().collect())
    new_source_files = all_sources - tracked_files

    if not new_source_files:
        log.info("No new source files to export. Gold table is up-to-date.")
        return

    log.info("Found %d new source file(s) to export.", len(new_source_files))

    # Filter to new source files only
    new_data = silver_df.filter(col("source_file").isin(new_source_files))
    new_row_count = new_data.count()
    log.info("New rows to export: %d.", new_row_count)

    if new_row_count == 0:
        return

    # Write new data to Gold UC table
    try:
        new_data.write.format("delta").mode("append").partitionBy("log_date").saveAsTable(GOLD_UC_PATH)

        log.info("Successfully wrote %d rows to %s.", new_row_count, GOLD_UC_PATH)
    except Exception as exc:
        log.error(
            "Write to %s failed: %s. Tracking table NOT updated.",
            GOLD_UC_PATH,
            exc,
        )
        raise

    # Update tracking table with newly exported source_files
    insert_tracking_records(spark, new_source_files)

    log.info(
        "Export complete. %d rows written to %s from %d new file(s).",
        new_row_count,
        GOLD_UC_PATH,
        len(new_source_files),
    )

    # Display summary (Databricks notebook output)
    try:
        display(spark.sql(f"SELECT COUNT(*) AS gold_rows FROM {GOLD_UC_PATH}"))
    except NameError:
        pass


def main():
    parser = argparse.ArgumentParser(description="Export Warehouse (Databricks UC): Silver Delta → Gold UC table")
    parser.parse_args()  # No CLI args needed — config is in constants above

    spark = create_spark_session()
    try:
        run(spark)
    except Exception:
        log.exception("Export warehouse job failed.")
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
