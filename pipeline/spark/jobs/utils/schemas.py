"""
PySpark StructType schemas for all medallion layers.

These schemas ensure column names, types and nullability are consistent
across the bronze → silver → gold pipeline. Each schema is imported by
the corresponding job module and used when creating DataFrames from RDDs
or writing Delta tables.
"""

from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
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

# ── Silver Layer — Enriched rows (Bronze + GeoIP + UA + computed) ──
# Matches silver_enrichment output (36 columns, partitioned by log_date).
silver_schema = StructType([
    *bronze_schema.fields,
    StructField("country", StringType(), True),
    StructField("region", StringType(), True),
    StructField("city", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("postcode", StringType(), True),
    StructField("isp", StringType(), True),
    StructField("agent_type", StringType(), True),
    StructField("browser_name", StringType(), True),
    StructField("browser_version", StringType(), True),
    StructField("operating_system", StringType(), True),
    StructField("device_type", StringType(), True),
    StructField("page_category", StringType(), True),
    StructField("referrer_domain", StringType(), True),
    StructField("traffic_type", StringType(), True),
    StructField("is_crawler", BooleanType(), True),
    StructField("size_band", StringType(), True),
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
