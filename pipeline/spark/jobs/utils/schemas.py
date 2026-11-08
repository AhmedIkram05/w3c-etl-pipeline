"""
PySpark StructType schemas for the medallion layers that build DataFrames
from an explicit schema.

``bronze_schema`` is what ``bronze_ingestion`` parses log lines into.
``gold_schema`` drives the column selection in ``export_warehouse``, so the
warehouse export and ``RAW_ENRICHED_DDL`` cannot drift apart.

Silver has no schema here on purpose: ``silver_enrichment`` derives its
columns with ``withColumn`` from bronze, and pinning a StructType would
change the Delta column order for existing tables.
"""

from pyspark.sql.types import (
    BooleanType,
    DateType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# ── Bronze Layer — Raw parsed W3C log rows ─────────────────────────
# Matches the `raw_logs` table schema in PostgreSQL.
bronze_schema = StructType([
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

# ── Gold Layer — Warehouse export (raw_enriched DDL subset) ───────
# The 25 exported columns live in export_warehouse.raw_enriched_cols.
gold_schema = StructType([
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
    StructField("postcode", StringType(), True),
    StructField("page_category", StringType(), True),
    StructField("referrer_domain", StringType(), True),
    StructField("traffic_type", StringType(), True),
    StructField("is_crawler", BooleanType(), True),
    StructField("size_band", StringType(), True),
])
