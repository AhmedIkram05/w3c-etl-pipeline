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
    "country",
    "region",
    "city",
    "latitude",
    "longitude",
    "isp",
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
    source_file VARCHAR(255) PRIMARY KEY,
    loaded_at DATETIME2 DEFAULT GETDATE()
);
"""

TRACKING_MIGRATION_SQL = """
IF NOT EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID('dbo.raw_enriched_loaded')
    AND name = 'loaded_at'
)
ALTER TABLE dbo.raw_enriched_loaded
ADD loaded_at DATETIME2 DEFAULT GETDATE();
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
            if ("not currently available" in err_str or "timeout" in err_str.lower()) and attempt < RETRY_ATTEMPTS - 1:
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

    raise RuntimeError(f"Cannot connect to Azure SQL after {RETRY_ATTEMPTS} attempts: {last_error}") from last_error


def execute_ddl(conn, ddl_statement):
    """Execute a DDL statement via pymssql."""
    with conn.cursor() as cursor:
        cursor.execute(ddl_statement)
    conn.commit()
    print("DDL executed successfully")


def ensure_tables_exist(conn):
    """Create target tables if they do not exist, then run migrations."""
    execute_ddl(conn, RAW_ENRICHED_DDL)
    execute_ddl(conn, TRACKING_DDL)
    # Migration: add loaded_at column for data freshness monitoring
    # (safe to run repeatedly — IF NOT EXISTS guard)
    execute_ddl(conn, TRACKING_MIGRATION_SQL)


def _is_missing_table_error(exc) -> bool:
    """True when SQL Server reports the table itself does not exist (error 208).

    pymssql server errors carry the message number in ``args[0]``.
    """
    # Annotation defeats mypy's getattr special-case, which types the result
    # as the default's tuple[()] and flags args[0] as out of range.
    args: tuple = getattr(exc, "args", ())
    return bool(args) and args[0] == 208


def get_loaded_source_files(conn):
    """Return a set of source_file values already in the tracking table.

    A missing tracking table (SQL Server error 208) is expected on the first
    run and yields an empty set. Any other read error propagates: a transient
    failure must never be misread as "nothing loaded yet", or the export
    would re-send all history.
    """
    loaded = set()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT source_file FROM dbo.raw_enriched_loaded")
            for row in cursor.fetchall():
                loaded.add(row[0])
        print(f"Found {len(loaded)} already-loaded source files")
    except Exception as e:
        if _is_missing_table_error(e):
            print("Tracking table does not exist yet (will create)")
            return set()
        raise
    return loaded


def insert_batch(conn, rows, table, commit=True):
    """Insert a batch of rows using executemany.

    Accepts both Spark ``Row`` objects (from ``DataFrame.collect()``) and
    plain ``dict`` rows (for small tracking-table inserts).  Detects the
    input type by checking for ``__fields__`` (Row) vs ``keys()`` (dict).

    Pass ``commit=False`` to keep the transaction open across several
    inserts (per-file atomicity); the caller then commits explicitly.
    """
    if not rows:
        return 0

    # Support Row objects (Spark collect) and dicts (tracking table)
    if hasattr(rows[0], "__fields__"):
        columns = list(rows[0].__fields__)
        params = [tuple(row) for row in rows]  # no asDict() overhead
    else:
        columns = list(rows[0].keys())
        params = [tuple(row[c] for c in columns) for row in rows]

    col_names = ", ".join(f"[{c}]" for c in columns)
    placeholders = ", ".join(["%s"] * len(columns))
    sql = f"INSERT INTO {table} ({col_names}) VALUES ({placeholders})"

    with conn.cursor() as cursor:
        cursor.executemany(sql, params)
    if commit:
        conn.commit()
    return len(rows)


def export_files_atomically(conn, rows_by_file, batch_size=BATCH_SIZE):
    """Insert rows and the tracking row for each source file, then commit.

    Grouping is ``{source_file: [rows]}``. For every file the data inserts
    and its ``dbo.raw_enriched_loaded`` tracking row share one transaction,
    so a mid-export crash leaves at most the in-flight file untracked and
    the Airflow retry skips every already-committed file instead of
    re-exporting the whole set.
    """
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc)
    inserted = 0
    for source_file in sorted(rows_by_file):
        file_rows = rows_by_file[source_file]
        for i in range(0, len(file_rows), batch_size):
            inserted += insert_batch(conn, file_rows[i : i + batch_size], "dbo.raw_enriched", commit=False)
        insert_batch(conn, [{"source_file": source_file, "loaded_at": now}], "dbo.raw_enriched_loaded", commit=False)
        conn.commit()
    return inserted


def export_to_azure_sql(spark, server, database, username, password):
    """Export new Silver data to Azure SQL with idempotency tracking."""
    # ── Connect ────────────────────────────────────────────────────────
    conn = _connect(server, database, username, password)

    try:
        # ── Ensure tables exist ───────────────────────────────────────
        ensure_tables_exist(conn)

        # ── Read from Silver via Unity Catalog ────────────────────────
        silver_df = spark.table("w3c_etl_databricks.silver.silver_enriched_logs")

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
            new_data_df = export_df.filter(~col("source_file").isin(list(loaded_files)))
        else:
            new_data_df = export_df

        # Collect all rows (avoids cache() — unsupported on serverless)
        new_rows = new_data_df.collect()
        new_row_count = len(new_rows)

        if new_row_count == 0:
            print("No new source files to export — Silver is up-to-date")
            return

        print(f"Exporting {new_row_count} rows ({len(loaded_files)} files already loaded)")
        new_source_files = set(row.source_file for row in new_rows)

        print(f"Collected {len(new_rows)} rows from {len(new_source_files)} new source file(s)")

        # ── Per-file INSERT + tracking in one transaction ────────────
        rows_by_file: dict = {}
        for row in new_rows:
            rows_by_file.setdefault(row.source_file, []).append(row)

        inserted = export_files_atomically(conn, rows_by_file)
        print(f"Successfully exported {inserted} rows to dbo.raw_enriched")

    finally:
        conn.close()
        print("Azure SQL connection closed")


def main():
    """Main entry point for Databricks serverless notebook."""
    spark = SparkSession.builder.appName("JDBC Export to Azure SQL").getOrCreate()

    try:
        # Retrieve credentials from Databricks secrets
        from pyspark.dbutils import DBUtils

        dbutils = DBUtils(spark)
        secrets_scope = "w3c-etl-pipeline"

        server = dbutils.secrets.get(scope=secrets_scope, key="azure.sql.server")
        database = dbutils.secrets.get(scope=secrets_scope, key="azure.sql.database")
        username = dbutils.secrets.get(scope=secrets_scope, key="azure.sql.username")
        password = dbutils.secrets.get(scope=secrets_scope, key="azure.sql.password")

        print(f"Server: {server}")
        print(f"Database: {database}")
        print(f"Username: {username}")

        # Execute export
        export_to_azure_sql(spark, server, database, username, password)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
