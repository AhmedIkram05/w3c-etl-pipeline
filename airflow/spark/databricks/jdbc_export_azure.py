"""
JDBC Export — Silver (Unity Catalog) → Azure SQL via pymssql
============================================================

Reads the Silver enriched Delta table from Unity Catalog using Spark,
collects rows, and batch-inserts new source files into Azure SQL
``dbo.raw_enriched`` via pymssql. Uses a tracking table
``dbo.raw_enriched_loaded`` for idempotency.

Why pymssql instead of df.write.jdbc()
--------------------------------------
On Databricks serverless compute (Spark Connect), the built-in
``sqlserver`` JDBC data source only supports READ operations. DML
writes (INSERT) are not available. pymssql is a pure-Python driver
that works on any serverless environment.

Usage
-----
Run as a Databricks notebook on serverless compute::

    databricks jobs run-now --job-id <job-id>

Requirements
------------
- Databricks serverless compute with ``pymssql>=2.2.11`` installed
  (configured as job environment dependency)
- Databricks secret scope ``w3c-etl-pipeline`` with keys:
  ``azure.sql.server``, ``azure.sql.database``, ``azure.sql.username``,
  ``azure.sql.password``
- Unity Catalog table ``w3c_etl_databricks.silver.silver_enriched_logs``
"""

import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when

# ── Exact column list from specification ─────────────────────────────────
EXPORT_COLUMNS = [
    "log_date", "log_time", "server_ip", "method", "uri_stem",
    "uri_query", "client_ip", "user_agent", "cookie", "referrer",
    "status", "sub_status", "win32_status", "bytes_sent", "bytes_recv",
    "server_port", "username", "time_taken", "source_file",
    "postcode", "page_category", "referrer_domain", "traffic_type",
    "is_crawler", "size_band",
    "country", "region", "city", "latitude", "longitude", "isp",
]

RAW_ENRICHED_DDL = """
IF OBJECT_ID('dbo.raw_enriched', 'U') IS NULL
CREATE TABLE dbo.raw_enriched (
    log_date         DATE,
    log_time         VARCHAR(20),
    server_ip        VARCHAR(45),
    method           VARCHAR(10),
    uri_stem         NVARCHAR(MAX),
    uri_query        NVARCHAR(MAX),
    client_ip        VARCHAR(45),
    user_agent       NVARCHAR(MAX),
    cookie           NVARCHAR(MAX),
    referrer         NVARCHAR(MAX),
    status           INT,
    sub_status       INT,
    win32_status     INT,
    bytes_sent       BIGINT,
    bytes_recv       BIGINT,
    server_port      INT,
    username         VARCHAR(100),
    time_taken       BIGINT,
    source_file      VARCHAR(255),
    postcode         VARCHAR(20),
    page_category    VARCHAR(50),
    referrer_domain  VARCHAR(255),
    traffic_type     VARCHAR(50),
    is_crawler       BIT,
    size_band        VARCHAR(20),
    country          VARCHAR(100),
    region           VARCHAR(100),
    city             VARCHAR(100),
    latitude         FLOAT,
    longitude        FLOAT,
    isp              VARCHAR(200)
);
"""

TRACKING_DDL = """
IF OBJECT_ID('dbo.raw_enriched_loaded', 'U') IS NULL
CREATE TABLE dbo.raw_enriched_loaded (
    source_file VARCHAR(255) PRIMARY KEY
);
"""

RETRY_ATTEMPTS = 4
BATCH_SIZE = 5000  # rows per INSERT batch


def _connect(server, database, username, password, timeout=30):
    """Create a pymssql connection with retry for DB auto-resume."""
    import pymssql

    last_error = None
    for attempt in range(RETRY_ATTEMPTS):
        try:
            conn = pymssql.connect(
                server=server,
                database=database,
                user=username,
                password=password,
                port=1433,
                login_timeout=timeout,
            )
            print(f"Connected to Azure SQL (attempt {attempt + 1})")
            return conn
        except pymssql.OperationalError as e:
            last_error = e
            err_str = str(e)
            if (
                "not currently available" in err_str
                or "timeout" in err_str.lower()
            ) and attempt < RETRY_ATTEMPTS - 1:
                backoff = 15 * (2**attempt)
                print(
                    f"Connection not available (attempt {attempt + 1}), "
                    f"retrying in {backoff}s — database may be resuming..."
                )
                time.sleep(backoff)
            else:
                break
        except Exception as e:
            last_error = e
            break

    raise RuntimeError(
        f"Cannot connect to Azure SQL after {RETRY_ATTEMPTS} attempts: {last_error}"
    ) from last_error


def execute_ddl(conn, ddl_statement):
    """Execute a DDL statement via pymssql."""
    with conn.cursor() as cursor:
        cursor.execute(ddl_statement)
    conn.commit()
    print("DDL executed successfully")


def ensure_tables_exist(conn):
    """Create target tables if they do not exist."""
    execute_ddl(conn, RAW_ENRICHED_DDL)
    execute_ddl(conn, TRACKING_DDL)


def get_loaded_source_files(conn):
    """Return a set of source_file values already in the tracking table."""
    loaded = set()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT source_file FROM dbo.raw_enriched_loaded")
            for row in cursor.fetchall():
                loaded.add(row[0])
        print(f"Found {len(loaded)} already-loaded source files")
    except Exception as e:
        print(f"Tracking table read failed (will create): {e}")
    return loaded


def insert_batch(conn, rows, table):
    """Insert a batch of rows using executemany.

    Accepts both Spark ``Row`` objects (from ``DataFrame.collect()``) and
    plain ``dict`` rows (for small tracking-table inserts).  Detects the
    input type by checking for ``__fields__`` (Row) vs ``keys()`` (dict).
    """
    if not rows:
        return 0

    # Support Row objects (Spark collect) and dicts (tracking table)
    if hasattr(rows[0], "__fields__"):
        columns = list(rows[0].__fields__)
        params = [tuple(row) for row in rows]          # no asDict() overhead
    else:
        columns = list(rows[0].keys())
        params = [tuple(row[c] for c in columns) for row in rows]

    col_names = ", ".join(f"[{c}]" for c in columns)
    placeholders = ", ".join(["%s"] * len(columns))
    sql = f"INSERT INTO {table} ({col_names}) VALUES ({placeholders})"

    with conn.cursor() as cursor:
        cursor.executemany(sql, params)
    conn.commit()
    return len(rows)


def export_to_azure_sql(spark, server, database, username, password):
    """Export new Silver data to Azure SQL with idempotency tracking."""
    # ── Connect ────────────────────────────────────────────────────────
    conn = _connect(server, database, username, password)

    try:
        # ── Ensure tables exist ───────────────────────────────────────
        ensure_tables_exist(conn)

        # ── Read from Silver via Unity Catalog ────────────────────────
        silver_df = spark.table(
            "w3c_etl_databricks.silver.silver_enriched_logs"
        )

        # Select and prepare export columns
        export_df = silver_df.select(EXPORT_COLUMNS)

        # Cast is_crawler from string ("true"/"false") to BIT (0/1)
        export_df = export_df.withColumn(
            "is_crawler",
            when(col("is_crawler") == "true", lit(1)).otherwise(lit(0)),
        )

        # ── Load already-exported files ───────────────────────────────
        loaded_files = get_loaded_source_files(conn)

        # ── Filter in Spark before collecting to driver ──────────────
        if loaded_files:
            new_data_df = export_df.filter(
                ~col("source_file").isin(list(loaded_files))
            )
        else:
            new_data_df = export_df

        # Collect all rows (avoids cache() — unsupported on serverless)
        new_rows = new_data_df.collect()
        new_row_count = len(new_rows)

        if new_row_count == 0:
            print("No new source files to export — Silver is up-to-date")
            return

        print(
            f"Exporting {new_row_count} rows "
            f"({len(loaded_files)} files already loaded)"
        )
        new_source_files = set(row.source_file for row in new_rows)

        print(
            f"Collected {len(new_rows)} rows from "
            f"{len(new_source_files)} new source file(s)"
        )

        # ── Batch INSERT into dbo.raw_enriched ───────────────────────
        inserted = 0
        for i in range(0, len(new_rows), BATCH_SIZE):
            batch = new_rows[i : i + BATCH_SIZE]
            count = insert_batch(conn, batch, "dbo.raw_enriched")
            inserted += count
            if (i // BATCH_SIZE) % 5 == 0 or i + BATCH_SIZE >= len(new_rows):
                print(f"  Inserted {inserted}/{new_row_count} rows...")

        print(f"Successfully exported {inserted} rows to dbo.raw_enriched")

        # ── Update tracking table ────────────────────────────────────
        tracking_rows = [
            {"source_file": sf} for sf in sorted(new_source_files)
        ]
        inserted_tracking = insert_batch(
            conn, tracking_rows, "dbo.raw_enriched_loaded"
        )
        print(
            f"Updated tracking table with {inserted_tracking} new source file(s)"
        )

    finally:
        conn.close()
        print("Azure SQL connection closed")


def main():
    """Main entry point for Databricks serverless notebook."""
    spark = SparkSession.builder \
        .appName("JDBC Export to Azure SQL") \
        .getOrCreate()

    try:
        # Retrieve credentials from Databricks secrets
        from pyspark.dbutils import DBUtils

        dbutils = DBUtils(spark)
        secrets_scope = "w3c-etl-pipeline"

        server = dbutils.secrets.get(
            scope=secrets_scope, key="azure.sql.server"
        )
        database = dbutils.secrets.get(
            scope=secrets_scope, key="azure.sql.database"
        )
        username = dbutils.secrets.get(
            scope=secrets_scope, key="azure.sql.username"
        )
        password = dbutils.secrets.get(
            scope=secrets_scope, key="azure.sql.password"
        )

        print(f"Server: {server}")
        print(f"Database: {database}")
        print(f"Username: {username}")

        # Execute export
        export_to_azure_sql(spark, server, database, username, password)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
