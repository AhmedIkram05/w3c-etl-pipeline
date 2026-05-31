"""
PySpark StructType schemas for all medallion layers.

These schemas ensure column names, types and nullability are consistent
across the bronze → silver → gold pipeline. Each schema is imported by
the corresponding job module and used when creating DataFrames from RDDs
or writing Delta tables.
"""

from pyspark.sql.types import (
    DateType,
    IntegerType,
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
    StructField("bytes_sent", IntegerType(), True),
    StructField("bytes_recv", IntegerType(), True),
    StructField("time_taken", IntegerType(), True),
    StructField("source_file", StringType(), True),
])
