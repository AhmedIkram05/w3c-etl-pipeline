"""
W3C Spark Ingestion Azure DAG — Cloud-Native Single Pipeline.

This DAG orchestrates the cloud-native ETL pipeline on Databricks + Azure SQL:

  1. Triggers the Databricks Workflow (``w3c-etl-workflow``), which runs:
       - Task 1: DLT Bronze pipeline (raw W3C log ingestion from ADLS Gen2)
       - Task 2: DLT Silver pipeline (GeoIP enrichment + computed fields)
       - Task 3: JDBC Export notebook (Silver → Azure SQL ``dbo.raw_enriched``)
  2. Builds Airflow-managed dimension tables (``dim_geolocation``,
     ``dim_useragent``) from Azure SQL - dim_geolocation as SCD Type 2,
     dim_useragent via MERGE upsert.

The Dataset outlet ``Dataset("mssql://azure-sql/dbo/raw_enriched_loaded")``
is designed to trigger a downstream ``dbt_marts_azure`` DAG for dbt
transformations against Azure SQL (not yet implemented).

Schedule
--------
Weekly — runs every Friday at 5:00 PM UTC. Scheduled via Airflow cron
expression (``schedule="0 17 * * 5"``).

Prerequisites
-------------
* Databricks Airflow connection ``databricks_default`` with:
    - Host: Databricks workspace URL
    - Token: Databricks PAT token
* Azure SQL environment variables (for dimension export):
    - ``AZURE_SQL_SERVER``
    - ``AZURE_SQL_DATABASE`` (default: ``w3c-etl-db``)
    - ``AZURE_SQL_USER``
    - ``AZURE_SQL_PASS``

If Azure SQL credentials are not configured, the dimension export task
logs a warning and returns gracefully — the main pipeline data is still
available in Azure SQL via the Databricks Workflow's JDBC export.
"""

from __future__ import annotations

import datetime as dt
import logging
import os
from urllib.parse import unquote_plus

from airflow import DAG
from airflow.datasets import Dataset
from airflow.operators.python import PythonOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator

logger = logging.getLogger(__name__)

# ── Dataset outlet — intended to signal downstream dbt_marts_azure DAG ─────
AZURE_WAREHOUSE_LOADED = Dataset("mssql://azure-sql/dbo/raw_enriched_loaded")

# ── Lineage datasets (OpenLineage) ─────────────────────────────────────────
# Declared as task inlets/outlets so apache-airflow-providers-openlineage
# converts them into OpenLineage input/output datasets — giving Marquez a
# cross-engine lineage graph (Databricks DLT → Azure SQL → dbt). The URIs
# reuse Airflow Dataset addressing so one definition drives both trigger
# semantics and lineage edges.
RAW_ENRICHED = Dataset("mssql://azure-sql/dbo/raw_enriched")
DIM_GEOLOCATION = Dataset("mssql://azure-sql/dbo/dim_geolocation")
DIM_USERAGENT = Dataset("mssql://azure-sql/dbo/dim_useragent")

# ── Databricks job ID (from terraform/databricks output.workflow_job_id) ─────
DATABRICKS_JOB_ID = os.environ.get("DATABRICKS_JOB_ID", "847995192336508")

# ── Default arguments ──────────────────────────────────────────────────────
default_args = {
    "owner": "w3c-team",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": dt.timedelta(minutes=5),
    "retry_exponential": True,
    "max_retry_delay": dt.timedelta(hours=1),
}


def _odbc_value(value: str) -> str:
    """Quote an ODBC connection-string value so ``;``, ``=`` and braces are safe.

    A braced value is literal except that every ``}`` must be doubled
    (``}}``); ``{`` needs no escaping. The braces also stop a ``;`` or ``=``
    inside the password from terminating the ``PWD=`` pair.
    """
    return "{" + value.replace("}", "}}") + "}"


def _azure_conn_str(server: str, database: str, username: str, password: str) -> str:
    """Build the Azure SQL ODBC connection string."""
    return (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={_odbc_value(password)};"
        f"Encrypt=yes;TrustServerCertificate=no;"
    )


def _device_type(parsed) -> str:
    """Classify a parsed user-agent into a dim_useragent device_type label."""
    if getattr(parsed, "is_mobile", False):
        return "Mobile"
    if getattr(parsed, "is_tablet", False):
        return "Tablet"
    if getattr(parsed, "is_pc", False):
        return "Desktop"
    if getattr(parsed, "is_bot", False):
        return "Bot"
    return "Other"


def _parse_user_agent_rows(ua_rows) -> list[tuple]:
    """Parse raw UA strings into deduplicated dim_useragent insert tuples.

    Returns ``[]`` when the ``user_agents`` library is unavailable so the
    caller can skip the merge without failing the DAG run.
    """
    try:
        import hashlib

        from user_agents import parse as ua_parse
    except ImportError:
        logger.warning("user-agents library not installed; skipping dim_useragent build")
        return []

    logger.info(f"Parsing {len(ua_rows)} distinct user-agent strings...")
    insert_data: list[tuple] = []
    seen_hashes: set = set()
    for idx, (ua_raw,) in enumerate(ua_rows):
        # Store the RAW (URL-encoded) string so it survives the round trip;
        # parse the decoded copy for browser/OS/device.
        ua_raw_str = ua_raw[:2048] if ua_raw else ""
        ua_str_decoded = unquote_plus(ua_raw_str)
        parsed = ua_parse(ua_str_decoded)

        agent_type = "Crawler" if getattr(parsed, "is_bot", False) else "Browser"
        browser_name = parsed.browser.family or "Unknown"
        browser_version = parsed.browser.version_string or "Unknown"
        os_name = parsed.os.family or "Unknown"
        device = _device_type(parsed)

        # Key on md5 of the FULL raw UA, matching dbt's tsql_ua_hash: the fact
        # joins on LOWER(HASHBYTES('MD5', c.user_agent)), so the stored hash is
        # lowercase utf-8 md5. Distinct raw strings now get distinct rows
        # instead of collapsing on a 1000-char prefix.
        ua_hash = hashlib.md5(
            (ua_raw or "").encode("utf-8"), usedforsecurity=False
        ).hexdigest()

        if ua_hash in seen_hashes:
            continue
        seen_hashes.add(ua_hash)

        insert_data.append((
            ua_hash,
            ua_raw_str,
            agent_type,
            browser_name,
            browser_version,
            os_name,
            device,
        ))

        if (idx + 1) % 500 == 0:
            logger.info(f"Parsed {idx + 1}/{len(ua_rows)} user-agents...")
    return insert_data


def _merge_useragent_rows(cursor, insert_data: list[tuple], batch_size: int = 250) -> None:
    """Batched MERGE upsert of dim_useragent rows.

    ``batch_size`` stays under SQL Server's 2100-parameter limit
    (7 parameters per row).
    """
    if not insert_data:
        return
    logger.info(f"Inserting {len(insert_data)} rows into dim_useragent...")
    for batch_start in range(0, len(insert_data), batch_size):
        batch = insert_data[batch_start : batch_start + batch_size]
        placeholders = ",".join(["(?,?,?,?,?,?,?)"] * len(batch))
        params = tuple(v for row in batch for v in row)
        cursor.execute(
            f"""
            MERGE dbo.dim_useragent AS target
            USING (VALUES {placeholders}) AS source (ua_hash, user_agent, agent_type, browser_name, browser_version, operating_system, device_type)
            ON target.ua_hash = source.ua_hash
            WHEN NOT MATCHED THEN
                INSERT (ua_hash, user_agent, agent_type, browser_name, browser_version, operating_system, device_type)
                VALUES (source.ua_hash, source.user_agent, source.agent_type, source.browser_name,
                        source.browser_version, source.operating_system, source.device_type);
            """,
            params,
        )
    logger.info(f"Inserted {len(insert_data)} rows into dim_useragent")


def _build_dim_geolocation(cursor, conn) -> None:
    """Create (or migrate) ``dim_geolocation`` and load the SCD Type 2 snapshot."""
    cursor.execute("""
        IF OBJECT_ID('dbo.dim_geolocation') IS NULL
        BEGIN
            CREATE TABLE dbo.dim_geolocation (
                geolocation_sk  INT IDENTITY(1,1) PRIMARY KEY,
                geo_hash        NVARCHAR(64)   NOT NULL,
                country         NVARCHAR(100)  NULL,
                region          NVARCHAR(100)  NULL,
                city            NVARCHAR(100)  NULL,
                latitude        FLOAT          NULL,
                longitude       FLOAT          NULL,
                isp             NVARCHAR(200)  NULL,
                -- SCD Type 2 validity columns
                valid_from      DATETIME2      NOT NULL
                    CONSTRAINT df_dim_geolocation__valid_from DEFAULT SYSUTCDATETIME(),
                valid_to        DATETIME2      NOT NULL
                    CONSTRAINT df_dim_geolocation__valid_to DEFAULT '9999-12-31',
                is_current      BIT            NOT NULL
                    CONSTRAINT df_dim_geolocation__is_current DEFAULT 1
            );

            -- SCD2: history allowed per geo_hash, but exactly ONE current
            -- version each (filtered unique index, not a table constraint).
            CREATE UNIQUE INDEX ux_dim_geolocation__current
                ON dbo.dim_geolocation (geo_hash) WHERE is_current = 1;

            -- Seed -1 sentinel unknown row for FK integrity
            SET IDENTITY_INSERT dbo.dim_geolocation ON;
            INSERT INTO dbo.dim_geolocation (geolocation_sk, geo_hash, country, region, city, isp)
            VALUES (-1, '0000000000000000000000000000000000000000000000000000000000000000', 'Unknown', 'Unknown', 'Unknown', '-');
            SET IDENTITY_INSERT dbo.dim_geolocation OFF;
        END;
        ELSE IF COL_LENGTH('dbo.dim_geolocation', 'valid_from') IS NULL
        BEGIN
            -- One-time migration of pre-SCD2 deployments:
            -- SCD1 unique constraint must go (history needs duplicate hashes),
            -- existing rows become the first current version via defaults.
            IF EXISTS (
                SELECT 1 FROM sys.key_constraints
                WHERE name = 'uq_dim_geolocation__geo_hash'
                  AND parent_object_id = OBJECT_ID('dbo.dim_geolocation')
            )
                ALTER TABLE dbo.dim_geolocation DROP CONSTRAINT uq_dim_geolocation__geo_hash;

            ALTER TABLE dbo.dim_geolocation ADD
                valid_from DATETIME2 NOT NULL
                    CONSTRAINT df_dim_geolocation__valid_from DEFAULT SYSUTCDATETIME(),
                valid_to   DATETIME2 NOT NULL
                    CONSTRAINT df_dim_geolocation__valid_to DEFAULT '9999-12-31',
                is_current BIT       NOT NULL
                    CONSTRAINT df_dim_geolocation__is_current DEFAULT 1;

            CREATE UNIQUE INDEX ux_dim_geolocation__current
                ON dbo.dim_geolocation (geo_hash) WHERE is_current = 1;
        END;
    """)

    # SCD Type 2 load:
    #   1. Aggregate current attributes per geo_hash into @src.
    #   2. MERGE against CURRENT rows only (ON ... is_current = 1):
    #        - new hash          -> insert new current version
    #        - hash present but
    #          tracked attr changed -> expire old version (valid_to /
    #          is_current = 0). T-SQL MERGE cannot expire AND insert in
    #          one branch, so the $action OUTPUT captures expired hashes
    #   3. Re-insert a fresh current version for every expired hash.
    # country/region/city/lat/long are baked into geo_hash; MAX(isp)
    # is the only tracked attribute outside it, so isp drift alone
    # triggers a new version.
    cursor.execute("""
        DECLARE @src TABLE (
            geo_hash NVARCHAR(64) NOT NULL PRIMARY KEY,
            country  NVARCHAR(100),
            region   NVARCHAR(100),
            city     NVARCHAR(100),
            latitude FLOAT,
            longitude FLOAT,
            isp      NVARCHAR(200)
        );

        INSERT INTO @src (geo_hash, country, region, city, latitude, longitude, isp)
        SELECT
            CONVERT(NVARCHAR(64), HASHBYTES('SHA2_256',
                ISNULL(country, '') + '|'
                + ISNULL(region, '') + '|'
                + ISNULL(city, '') + '|'
                + ISNULL(CONVERT(NVARCHAR(32), CONVERT(DECIMAL(18,10), latitude)), '') + '|'
                + ISNULL(CONVERT(NVARCHAR(32), CONVERT(DECIMAL(18,10), longitude)), '')
            ), 2)                                               AS geo_hash,
            country, region, city, latitude, longitude,
            MAX(isp)                                            AS isp
        FROM dbo.raw_enriched
        WHERE country IS NOT NULL
        GROUP BY country, region, city, latitude, longitude;

        DECLARE @scd2_log TABLE (merge_action NVARCHAR(10), geo_hash NVARCHAR(64));

        MERGE dbo.dim_geolocation AS target
        USING @src AS source
        ON target.geo_hash = source.geo_hash AND target.is_current = 1
        WHEN MATCHED AND ISNULL(target.isp, '') <> ISNULL(source.isp, '') THEN
            UPDATE SET
                valid_to   = SYSUTCDATETIME(),
                is_current = 0
        WHEN NOT MATCHED THEN
            INSERT (geo_hash, country, region, city, latitude, longitude, isp)
            VALUES (source.geo_hash, source.country, source.region,
                    source.city, source.latitude, source.longitude, source.isp)
        OUTPUT $action, inserted.geo_hash INTO @scd2_log (merge_action, geo_hash);

        INSERT INTO dbo.dim_geolocation (geo_hash, country, region, city, latitude, longitude, isp)
        SELECT s.geo_hash, s.country, s.region, s.city, s.latitude, s.longitude, s.isp
        FROM @src s
        WHERE EXISTS (
            SELECT 1 FROM @scd2_log l
            WHERE l.merge_action = 'UPDATE' AND l.geo_hash = s.geo_hash
        );
    """)
    conn.commit()


def _build_dim_useragent(cursor, conn) -> None:
    """Create (or migrate) ``dim_useragent`` and MERGE parsed UA rows."""
    # ── dim_useragent (MERGE upsert on ua_hash) ─────────────
    # Parse raw user-agent strings from dbo.raw_enriched using user-agents library
    # (matching the pattern from pipeline/plugins/operators/export_dimensions.py)
    cursor.execute("""
        IF OBJECT_ID('dbo.dim_useragent') IS NULL
        BEGIN
            CREATE TABLE dbo.dim_useragent (
                user_agent_sk   INT IDENTITY(1,1) PRIMARY KEY,
                ua_hash         NVARCHAR(64)   NOT NULL,
                user_agent      NVARCHAR(2048) NULL,
                agent_type      NVARCHAR(50)   NULL,
                browser_name    NVARCHAR(100)  NULL,
                browser_version NVARCHAR(50)   NULL,
                operating_system NVARCHAR(100)  NULL,
                device_type     NVARCHAR(50)   NULL,
                CONSTRAINT uq_dim_useragent__ua_hash
                    UNIQUE (ua_hash)
            );

            -- Seed -1 sentinel unknown row for FK integrity
            SET IDENTITY_INSERT dbo.dim_useragent ON;
            INSERT INTO dbo.dim_useragent (user_agent_sk, ua_hash, user_agent, agent_type, browser_name, browser_version, operating_system, device_type)
            VALUES (-1, '0000000000000000000000000000000000000000000000000000000000000000', 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown');
            SET IDENTITY_INSERT dbo.dim_useragent OFF;
        END;
    """)

    # Read distinct user_agent strings from dbo.raw_enriched
    cursor.execute(
        "SELECT DISTINCT user_agent FROM dbo.raw_enriched WHERE user_agent IS NOT NULL AND user_agent != '-'"
    )
    ua_rows = cursor.fetchall()

    if ua_rows:
        _merge_useragent_rows(cursor, _parse_user_agent_rows(ua_rows))
    else:
        logger.info("No user-agent strings found in dbo.raw_enriched; skipping dim_useragent build")

    conn.commit()


def _emit_outlets(context: dict, *datasets) -> None:
    """Emit Dataset outlets programmatically — only on real success.

    Outlets are deliberately NOT declared statically on the operators: a
    declared outlet fires whenever the task succeeds, even when the callable
    took a graceful skip (e.g. missing Azure SQL credentials), which would
    wake the downstream ``w3c_dbt_marts_azure`` DAG against stale data.
    Airflow >= 2.5 injects ``outlet_events`` into the task context; emitting
    here means downstream dataset consumers only trigger when the work
    actually happened. Direct invocations without a task context (unit
    tests) simply emit nothing.
    """
    outlet_events = context.get("outlet_events")
    if outlet_events is None:
        return
    for ds in datasets:
        outlet_events[ds].emit()


def _export_dimensions(**context) -> None:
    """Build Airflow-managed dimension tables from Azure SQL.

    Reads enriched web request data from Azure SQL ``dbo.raw_enriched``
    and builds:
      - ``dim_geolocation`` — unique geographic locations from client IPs
      - ``dim_useragent`` — parsed user-agent components

    Uses ``MERGE`` upsert on natural keys for idempotent incremental loads.

    Slowly Changing Dimension strategy:
      - ``dim_geolocation`` — **SCD Type 2**: attribute changes expire the
        current row (``valid_to`` set, ``is_current = 0``) and insert a new
        version, preserving full history per ``geo_hash``.
      - ``dim_useragent``   — SCD Type 1 (in-place MERGE upsert).

    In local development (where Azure SQL may not be accessible), the task
    logs a warning and exits gracefully — the Databricks Workflow's JDBC
    export already delivers the core data to Azure SQL.

    Environment variables
    ---------------------
    AZURE_SQL_SERVER : str
        Azure SQL server FQDN (e.g. ``w3c-etl-server.database.windows.net``).
    AZURE_SQL_DATABASE : str
        Azure SQL database name (default ``w3c-etl-db``).
    AZURE_SQL_USER : str
        Azure SQL admin / service principal username.
    AZURE_SQL_PASS : str
        Azure SQL password.
    """
    server = os.environ.get("AZURE_SQL_SERVER", "")
    database = os.environ.get("AZURE_SQL_DATABASE") or os.environ.get("AZURE_SQL_DB", "w3c-etl-db")
    username = os.environ.get("AZURE_SQL_USER", "")
    password = os.environ.get("AZURE_SQL_PASS", "")

    if not all([server, username, password]):
        logger.warning(
            "Azure SQL credentials not configured (set AZURE_SQL_SERVER, "
            "AZURE_SQL_USER, AZURE_SQL_PASS env vars). Skipping dimension "
            "export. The Databricks Workflow JDBC export has already "
            "delivered data to Azure SQL."
        )
        return

    try:
        import pyodbc

        with pyodbc.connect(_azure_conn_str(server, database, username, password), autocommit=False) as conn:
            cursor = conn.cursor()
            _build_dim_geolocation(cursor, conn)
            _build_dim_useragent(cursor, conn)

        logger.info("Dimension tables dim_geolocation and dim_useragent built successfully from Azure SQL.")
        _emit_outlets(context, DIM_GEOLOCATION, DIM_USERAGENT)

    except ImportError:
        logger.warning(
            "pyodbc not installed — cannot build dimension tables from "
            "Azure SQL. Install pyodbc or run inside the Airflow container."
        )
    except pyodbc.Error:
        logger.exception(
            "Azure SQL error while building dimension tables. "
            "The core pipeline data is still available in dbo.raw_enriched."
        )
        raise


def _create_indexes(**context) -> None:
    """Create indexes on Azure SQL tables to speed up dbt queries.

    Runs after dimensions are built and before the Dataset fires to trigger
    the downstream ``dbt_marts_azure`` DAG.

    Indexes created:
      - ``ix_raw_enriched_cx``: Clustered index on ``dbo.raw_enriched(log_date, source_file, log_time)``.
        The leading ``log_date`` matches the incremental WHERE clause in
        ``fact_webrequest.sql`` (``log_date >= watermark``) so the slice scan is
        index-ordered instead of an unpruned table scan.
      - ``ix_raw_enriched_client_ip``: Non-clustered index on client_ip speeds up
        the ``ip_visit_buckets`` semi-join in ``fact_webrequest.sql``.

    All index creation uses ``IF NOT EXISTS`` checks for idempotent re-runs.

    Environment variables
    ---------------------
    Same as ``_export_dimensions`` — ``AZURE_SQL_SERVER``, ``AZURE_SQL_DATABASE``,
    ``AZURE_SQL_USER``, ``AZURE_SQL_PASS``.
    """
    server = os.environ.get("AZURE_SQL_SERVER", "")
    database = os.environ.get("AZURE_SQL_DATABASE") or os.environ.get("AZURE_SQL_DB", "w3c-etl-db")
    username = os.environ.get("AZURE_SQL_USER", "")
    password = os.environ.get("AZURE_SQL_PASS", "")

    if not all([server, username, password]):
        logger.warning("Azure SQL credentials not configured. Skipping index creation.")
        return

    try:
        import pyodbc

        conn_str = _azure_conn_str(server, database, username, password)

        with pyodbc.connect(conn_str, autocommit=True) as conn:
            cursor = conn.cursor()

            # Clustered index on raw_enriched, leading with log_date so the
            # incremental log_date >= watermark predicate is index-ordered.
            # The existence check verifies the leading column: deployments
            # carrying the legacy (source_file, log_time) key are rebuilt.
            cursor.execute("""
                IF NOT EXISTS (
                    SELECT 1 FROM sys.indexes i
                    JOIN sys.index_columns ic
                        ON ic.object_id = i.object_id AND ic.index_id = i.index_id AND ic.key_ordinal = 1
                    JOIN sys.columns c
                        ON c.object_id = ic.object_id AND c.column_id = ic.column_id
                    WHERE i.name = 'ix_raw_enriched_cx'
                      AND i.object_id = OBJECT_ID('dbo.raw_enriched')
                      AND c.name = 'log_date'
                )
                BEGIN
                    DROP INDEX IF EXISTS ix_raw_enriched_cx ON dbo.raw_enriched;
                    CREATE CLUSTERED INDEX ix_raw_enriched_cx
                    ON dbo.raw_enriched(log_date, source_file, log_time);
                    PRINT 'Created ix_raw_enriched_cx (leading log_date)';
                END
                ELSE
                    PRINT 'ix_raw_enriched_cx already exists';
            """)
            logger.info("Clustered index ix_raw_enriched_cx created (or already exists)")

            # Non-clustered index on raw_enriched(client_ip)
            cursor.execute("""
                IF NOT EXISTS (
                    SELECT 1 FROM sys.indexes
                    WHERE name = 'ix_raw_enriched_client_ip'
                      AND object_id = OBJECT_ID('dbo.raw_enriched')
                )
                BEGIN
                    CREATE INDEX ix_raw_enriched_client_ip
                    ON dbo.raw_enriched(client_ip);
                    PRINT 'Created ix_raw_enriched_client_ip';
                END
                ELSE
                    PRINT 'ix_raw_enriched_client_ip already exists';
            """)
            logger.info("Index ix_raw_enriched_client_ip created (or already exists)")

            # Index on dim_geolocation(geo_hash) for efficient lookups
            cursor.execute("""
                IF NOT EXISTS (
                    SELECT 1 FROM sys.indexes
                    WHERE name = 'ix_dim_geolocation_geo_hash'
                      AND object_id = OBJECT_ID('dbo.dim_geolocation')
                )
                BEGIN
                    CREATE INDEX ix_dim_geolocation_geo_hash
                    ON dbo.dim_geolocation(geo_hash);
                    PRINT 'Created ix_dim_geolocation_geo_hash';
                END
                ELSE
                    PRINT 'ix_dim_geolocation_geo_hash already exists';
            """)

            # Index on dim_useragent(user_agent) for efficient lookups
            cursor.execute("""
                IF NOT EXISTS (
                    SELECT 1 FROM sys.indexes
                    WHERE name = 'ix_dim_useragent_user_agent'
                      AND object_id = OBJECT_ID('dbo.dim_useragent')
                )
                BEGIN
                    CREATE INDEX ix_dim_useragent_user_agent
                    ON dbo.dim_useragent(user_agent);
                    PRINT 'Created ix_dim_useragent_user_agent';
                END
                ELSE
                    PRINT 'ix_dim_useragent_user_agent already exists';
            """)

            conn.commit()

        logger.info("Azure SQL indexes created successfully.")
        _emit_outlets(context, AZURE_WAREHOUSE_LOADED)

    except Exception as e:
        logger.exception(f"Error while creating indexes: {e}")
        raise


# ── DAG definition ─────────────────────────────────────────────────────────
dag = DAG(
    dag_id="w3c_spark_ingestion_azure",
    schedule="0 17 * * 5",  # Friday at 5:00 PM UTC
    start_date=dt.datetime(2026, 3, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    description="Cloud-native ETL: Databricks DLT pipeline -> Azure SQL -> dimension export",
    tags=["w3c", "azure", "databricks", "cloud-native"],
)

# ════════════════════════════════════════════════════════════════════════════
# TASK 1: Trigger Databricks Workflow (Bronze → Silver → JDBC Export)
# ════════════════════════════════════════════════════════════════════════════

bronze_silver_jdbc_pipeline = DatabricksRunNowOperator(
    task_id="bronze_silver_jdbc_pipeline",
    databricks_conn_id="databricks_default",
    job_id=DATABRICKS_JOB_ID,
    outlets=[RAW_ENRICHED],
    dag=dag,
)

# ════════════════════════════════════════════════════════════════════════════
# TASK 2: Build Airflow-Managed Dimensions from Azure SQL
# ════════════════════════════════════════════════════════════════════════════

export_dimensions = PythonOperator(
    task_id="export_dimensions",
    python_callable=_export_dimensions,
    inlets=[RAW_ENRICHED],
    # Outlets (DIM_GEOLOCATION, DIM_USERAGENT) are emitted programmatically
    # inside _export_dimensions on real success — see _emit_outlets.
    # Bound the worker slot: a hung SCD2/UA build must not pin the weekly
    # Friday slot indefinitely.
    execution_timeout=dt.timedelta(hours=1),
    dag=dag,
)

# ════════════════════════════════════════════════════════════════════════════
# TASK 3: Create Indexes for dbt Performance
# ════════════════════════════════════════════════════════════════════════════

create_indexes = PythonOperator(
    task_id="create_indexes",
    python_callable=_create_indexes,
    # Outlet (AZURE_WAREHOUSE_LOADED) is emitted programmatically inside
    # _create_indexes on real success — see _emit_outlets.
    # The clustered-index rebuild is bounded so a hung DDL cannot pin the slot.
    execution_timeout=dt.timedelta(hours=1),
    dag=dag,
)

# ════════════════════════════════════════════════════════════════════════════
# Dependencies
# ════════════════════════════════════════════════════════════════════════════

bronze_silver_jdbc_pipeline >> export_dimensions >> create_indexes
