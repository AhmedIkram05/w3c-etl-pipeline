"""
Export Warehouse — Silver -> PostgreSQL via Spark JDBC.

Reads the Silver Delta table, applies type casting for PostgreSQL
compatibility, and writes enriched rows to ``public.raw_enriched``
using the Spark JDBC connector.  Idempotent via a tracking table
(``public.raw_enriched_loaded``) that records which ``source_file``
values have already been exported.

Usage (via ``spark-submit`` --with-postgresql-jar)::

    spark-submit --jars /opt/spark/jars/postgresql-42.7.4.jar \\
        export_warehouse.py \\
        --delta-dir /opt/spark/delta \\
        --jdbc-url jdbc:postgresql://postgres:5432/w3c_warehouse \\
        --jdbc-user "\\${W3C_DB_USER}" \\
        --jdbc-password "\\${W3C_DB_PASS}" \\
        --jdbc-driver org.postgresql.Driver

Idempotency
-----------
Before writing, the script queries ``public.raw_enriched_loaded`` for
already-exported ``source_file`` values.  Only rows whose ``source_file``
is not in that set are written.  After a successful JDBC write, the
new ``source_file`` values are inserted into the tracking table.

Type mappings (Delta -> PostgreSQL)
-----------------------------------
- ``is_crawler`` (StringType "true"/"false") -> BOOLEAN
- ``latitude`` / ``longitude`` (StringType)     -> DOUBLE PRECISION
- ``bytes_sent`` / ``bytes_recv`` / ``time_taken`` (LongType) -> BIGINT
"""

import argparse
import logging
import os
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lower, trim, when
from pyspark.sql.types import LongType

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("export_warehouse")

# ── Target table / tracking table ────────────────────────────────────

RAW_ENRICHED_TABLE = "public.raw_enriched"
TRACKING_TABLE = "public.raw_enriched_loaded"

# DDL for the enriched data table (36 columns matching silver schema).
RAW_ENRICHED_DDL = """
CREATE TABLE IF NOT EXISTS public.raw_enriched (
    log_date DATE,
    log_time TEXT,
    server_ip TEXT,
    method TEXT,
    uri_stem TEXT,
    uri_query TEXT,
    client_ip TEXT,
    user_agent TEXT,
    cookie TEXT,
    referrer TEXT,
    status INT,
    sub_status INT,
    win32_status INT,
    bytes_sent BIGINT,
    bytes_recv BIGINT,
    server_port INT,
    username TEXT,
    time_taken BIGINT,
    source_file TEXT,
    postcode TEXT,
    page_category TEXT,
    referrer_domain TEXT,
    traffic_type TEXT,
    is_crawler BOOLEAN,
    size_band TEXT
);
"""

# DDL for the idempotency tracking table.
TRACKING_DDL = """
CREATE TABLE IF NOT EXISTS public.raw_enriched_loaded (
    source_file TEXT PRIMARY KEY
);
"""


# ── Helpers ──────────────────────────────────────────────────────────


def create_spark_session(app_name: str = "W3C_Export_Warehouse") -> SparkSession:
    """Build a SparkSession with Delta Lake support."""
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )


def get_loaded_source_files(spark: SparkSession, jdbc_url: str, jdbc_props: dict) -> set:
    """Return the set of ``source_file`` values already in the tracking table.

    Returns an empty set if the table does not exist yet.
    """
    try:
        df = (
            spark.read
            .format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", TRACKING_TABLE)
            .options(**jdbc_props)
            .load()
        )
        rows = df.select("source_file").collect()
        return {row.source_file for row in rows}
    except Exception:
        log.info("Tracking table %s does not exist yet — will create.", TRACKING_TABLE)
        return set()


def _parse_jdbc_url(url: str) -> dict:
    """Parse a JDBC URL of the form ``jdbc:postgresql://host:port/dbname``.

    Returns a dict with ``host``, ``port``, and ``dbname`` suitable for
    ``psycopg2.connect()``.
    """
    from urllib.parse import urlparse

    # Strip the ``jdbc:`` prefix so urlparse sees a standard ``postgresql://`` URL.
    inner = url.removeprefix("jdbc:")
    parsed = urlparse(inner)
    return {
        "host": parsed.hostname or "localhost",
        "port": parsed.port or 5432,
        "dbname": parsed.path.lstrip("/"),
    }


def ensure_database_exists(
    jdbc_url: str,
    jdbc_props: dict,
):
    """Create the target database if it does not exist.

    PostgreSQL does not support ``CREATE DATABASE IF NOT EXISTS``, so we
    connect to the always-present ``postgres`` database first, check
    ``pg_database``, and issue ``CREATE DATABASE`` only when missing.
    This makes the pipeline self-provisioning after ``make clean``.
    """

    import psycopg2

    conn_info = _parse_jdbc_url(jdbc_url)
    target_db = conn_info["dbname"]

    # Connect to the bootstrap database (always present in a PG cluster).
    conn = None
    cur = None
    try:
        conn = psycopg2.connect(
            host=conn_info["host"],
            port=conn_info["port"],
            dbname="postgres",
            user=jdbc_props["user"],
            password=jdbc_props["password"],
        )
        conn.autocommit = True  # CREATE DATABASE cannot run inside a transaction.
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (target_db,))
        exists = cur.fetchone() is not None
        if not exists:
            cur.execute(f'CREATE DATABASE "{target_db}"')
            log.info("Created database '%s' (did not exist).", target_db)
        else:
            log.info("Database '%s' already exists.", target_db)
    except Exception as exc:
        log.warning("Could not ensure database '%s' exists: %s", target_db, exc)
    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()


def execute_ddl(
    spark: SparkSession,
    jdbc_url: str,
    jdbc_props: dict,
    ddl: str,
    table_label: str,
):
    """Execute a DDL statement via psycopg2 (Python-native PostgreSQL driver).

    Spark's DataFrame JDBC writer cannot run arbitrary DDL.  We use psycopg2
    directly because the Py4J gateway uses the JVM system classloader, which
    does not see Ivy-resolved jars (``spark.jars.packages``).
    """
    import psycopg2

    conn_info = _parse_jdbc_url(jdbc_url)
    conn = None
    cur = None
    try:
        conn = psycopg2.connect(
            host=conn_info["host"],
            port=conn_info["port"],
            dbname=conn_info["dbname"],
            user=jdbc_props["user"],
            password=jdbc_props["password"],
        )
        cur = conn.cursor()
        cur.execute(ddl)
        conn.commit()
        log.info("Executed DDL for %s", table_label)
    except Exception as exc:
        log.error("Failed to execute DDL for %s: %s", table_label, exc)
        raise
    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()


def apply_type_casts(df: DataFrame) -> DataFrame:
    """Cast Silver columns to PostgreSQL-compatible types.

    Transformations applied:
    - ``is_crawler`` (StringType "true"/"false") -> BOOLEAN
    - ``latitude``, ``longitude`` (StringType)   -> DOUBLE PRECISION
    - ``bytes_sent``, ``bytes_recv`` (LongType)   -> BIGINT
    - ``time_taken`` (LongType)                   -> BIGINT

    Columns that are absent from the DataFrame schema are silently skipped
    so that unit tests exercising a single column still work correctly.
    """
    cols = {f.name for f in df.schema.fields}

    if "is_crawler" in cols:
        df = df.withColumn(
            "is_crawler",
            when(trim(lower(col("is_crawler"))) == "true", True).otherwise(False),
        )

    if "bytes_sent" in cols:
        df = df.withColumn("bytes_sent", col("bytes_sent").cast(LongType()))
    if "bytes_recv" in cols:
        df = df.withColumn("bytes_recv", col("bytes_recv").cast(LongType()))
    if "time_taken" in cols:
        df = df.withColumn("time_taken", col("time_taken").cast(LongType()))
    return df


def insert_tracking_records(
    spark: SparkSession,
    jdbc_url: str,
    jdbc_props: dict,
    new_source_files: set,
):
    """Insert new ``source_file`` values into the tracking table.

    Uses a batch JDBC approach: creates a temporary DataFrame with the
    new values and appends it to ``public.raw_enriched_loaded``.
    """
    if not new_source_files:
        return

    from pyspark.sql.types import StringType, StructField, StructType

    tracking_schema = StructType([StructField("source_file", StringType(), True)])
    rows = [[sf] for sf in new_source_files]
    tracking_df = spark.createDataFrame(rows, schema=tracking_schema)

    tracking_df.write.format("jdbc").mode("append").option("url", jdbc_url).option("dbtable", TRACKING_TABLE).options(
        **jdbc_props
    ).save()

    log.info("Inserted %d new source_file(s) into %s.", len(new_source_files), TRACKING_TABLE)


# ── Main pipeline ────────────────────────────────────────────────────


def run(
    spark: SparkSession,
    delta_dir: str,
    jdbc_url: str,
    jdbc_user: str,
    jdbc_password: str,
    jdbc_driver: str,
):
    """Execute the Silver -> PostgreSQL export pipeline.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session.
    delta_dir : str
        Root directory containing ``silver/`` Delta table.
    jdbc_url : str
        JDBC connection URL for PostgreSQL.
    jdbc_user : str
        PostgreSQL user name.
    jdbc_password : str
        PostgreSQL password.
    jdbc_driver : str
        JDBC driver class name (e.g. ``org.postgresql.Driver``).
    """
    silver_path = os.path.join(delta_dir, "silver")
    jdbc_props = {
        "user": jdbc_user,
        "password": jdbc_password,
        "driver": jdbc_driver,
    }

    # ── 1. Read Silver Delta table ────────────────────────────────────
    try:
        silver_df: DataFrame = spark.read.format("delta").load(silver_path)
    except Exception as exc:
        log.error("Cannot read Silver Delta table at %s: %s", silver_path, exc)
        return

    total_rows = silver_df.count()
    if total_rows == 0:
        log.warning("Silver table is empty. Nothing to export.")
        return

    log.info("Loaded Silver with %d rows.", total_rows)

    # ── 2. Ensure target database exists (self-provisioning) ─────────
    ensure_database_exists(jdbc_url, jdbc_props)

    # ── 3. Determine new source_files (not yet in tracking table) ────
    tracked_files = get_loaded_source_files(spark, jdbc_url, jdbc_props)
    log.info("Already tracked source_files: %d.", len(tracked_files))

    all_sources = set(row.source_file for row in silver_df.select("source_file").distinct().collect())
    new_source_files = all_sources - tracked_files

    if not new_source_files:
        log.info("No new source files to export. All files already in PostgreSQL.")
        return

    log.info("Found %d new source file(s) to export.", len(new_source_files))

    # Filter silver data to only new source_files
    new_data = silver_df.filter(col("source_file").isin(new_source_files))
    new_row_count = new_data.count()
    log.info("New rows to export: %d.", new_row_count)

    if new_row_count == 0:
        return

    # ── 4. Apply type casting ────────────────────────────────────────
    cast_data = apply_type_casts(new_data)

    # ── 5. Select only the columns defined in the DDL ─────────────────
    # The Delta silver table has 36 columns (including geo/UA denorm fields),
    # but raw_enriched DDL only defines 25. Drop the extra 11 columns to
    # avoid COLUMN_NOT_DEFINED_IN_TABLE JDBC errors.
    raw_enriched_cols = [
        "log_date",
        "log_time",
        "server_ip",
        "method",
        "uri_stem",
        "uri_query",
        "client_ip",
        "user_agent",
        "cookie",
        "referrer",
        "status",
        "sub_status",
        "win32_status",
        "bytes_sent",
        "bytes_recv",
        "server_port",
        "username",
        "time_taken",
        "source_file",
        "postcode",
        "page_category",
        "referrer_domain",
        "traffic_type",
        "is_crawler",
        "size_band",
    ]
    cast_data = cast_data.select(*raw_enriched_cols)

    # ── 6. Create PostgreSQL tables if they do not exist ─────────────
    execute_ddl(spark, jdbc_url, jdbc_props, RAW_ENRICHED_DDL, RAW_ENRICHED_TABLE)
    execute_ddl(spark, jdbc_url, jdbc_props, TRACKING_DDL, TRACKING_TABLE)

    # ── 7. Write new data to PostgreSQL via JDBC ─────────────────────
    try:
        cast_data.write.format("jdbc").mode("append").option("url", jdbc_url).option(
            "dbtable", RAW_ENRICHED_TABLE
        ).options(**jdbc_props).save()

        log.info("Successfully wrote %d rows to %s.", new_row_count, RAW_ENRICHED_TABLE)
    except Exception as exc:
        log.error(
            "JDBC write to %s failed: %s. Tracking table NOT updated.",
            RAW_ENRICHED_TABLE,
            exc,
        )
        raise

    # ── 8. Update tracking table with newly exported source_files ────
    insert_tracking_records(spark, jdbc_url, jdbc_props, new_source_files)

    log.info(
        "Export complete. %d rows written to %s from %d new file(s).",
        new_row_count,
        RAW_ENRICHED_TABLE,
        len(new_source_files),
    )


def main():
    parser = argparse.ArgumentParser(description="Export Warehouse: Silver → PostgreSQL via Spark JDBC")
    parser.add_argument(
        "--delta-dir",
        default="/opt/spark/delta",
        help="Root Delta Lake directory (contains silver/). (default: %(default)s)",
    )
    parser.add_argument(
        "--jdbc-url",
        required=True,
        help="JDBC connection URL for PostgreSQL (e.g. jdbc:postgresql://host:5432/db)",
    )
    parser.add_argument(
        "--jdbc-user",
        required=True,
        help="PostgreSQL user name.",
    )
    parser.add_argument(
        "--jdbc-password",
        required=True,
        help="PostgreSQL password.",
    )
    parser.add_argument(
        "--jdbc-driver",
        default="org.postgresql.Driver",
        help="JDBC driver class name. (default: %(default)s)",
    )
    args = parser.parse_args()

    spark = create_spark_session()
    try:
        run(
            spark,
            args.delta_dir,
            args.jdbc_url,
            args.jdbc_user,
            args.jdbc_password,
            args.jdbc_driver,
        )
    except Exception:
        log.exception("Export warehouse job failed.")
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
