"""
W3C Spark Ingestion Azure DAG — Cloud-Native Single Pipeline.

This DAG orchestrates the cloud-native ETL pipeline on Databricks + Azure SQL:

  1. Triggers the Databricks Workflow (``w3c-etl-workflow``), which runs:
       - Task 1: DLT Bronze pipeline (raw W3C log ingestion from ADLS Gen2)
       - Task 2: DLT Silver pipeline (GeoIP enrichment + computed fields)
       - Task 3: JDBC Export notebook (Silver → Azure SQL ``dbo.raw_enriched``)
  2. Builds Airflow-managed dimension tables (``dim_geolocation``,
     ``dim_useragent``) from Azure SQL using MERGE upsert.

The Dataset outlet ``Dataset("mssql://azure-sql/dbo/raw_enriched_loaded")``
is designed to trigger a downstream ``dbt_marts_azure`` DAG for dbt
transformations against Azure SQL (not yet implemented).

Schedule
--------
Daily at 2:00 AM UTC — matching the Databricks Workflow schedule defined
in ``terraform/part_b/main.tf``.

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

from airflow.datasets import Dataset
from airflow.operators.python import PythonOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator

from airflow import DAG

logger = logging.getLogger(__name__)

# ── Dataset outlet — intended to signal downstream dbt_marts_azure DAG ─────
AZURE_WAREHOUSE_LOADED = Dataset("mssql://azure-sql/dbo/raw_enriched_loaded")

# ── Databricks job ID (from terraform/part_b output.workflow_job_id) ────────
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


def _export_dimensions(**context) -> None:
    """Build Airflow-managed dimension tables from Azure SQL.

    Reads enriched web request data from Azure SQL ``dbo.raw_enriched``
    and builds:
      - ``dim_geolocation`` — unique geographic locations from client IPs
      - ``dim_useragent`` — parsed user-agent components

    Uses ``MERGE`` upsert on natural keys for idempotent incremental loads.

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
    database = os.environ.get("AZURE_SQL_DATABASE", "w3c-etl-db")
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

        conn_str = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"UID={username};"
            f"PWD={password};"
            f"Encrypt=yes;TrustServerCertificate=no;"
        )

        with pyodbc.connect(conn_str, autocommit=False) as conn:
            cursor = conn.cursor()

            # ── dim_geolocation (MERGE upsert on geo_hash) ──────────
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
                        CONSTRAINT uq_dim_geolocation__geo_hash
                            UNIQUE (geo_hash)
                    );

                    -- Seed -1 sentinel unknown row for FK integrity
                    SET IDENTITY_INSERT dbo.dim_geolocation ON;
                    INSERT INTO dbo.dim_geolocation (geolocation_sk, geo_hash, country, region, city, isp)
                    VALUES (-1, '0000000000000000000000000000000000000000000000000000000000000000', 'Unknown', 'Unknown', 'Unknown', '-');
                    SET IDENTITY_INSERT dbo.dim_geolocation OFF;
                END;
            """)

            cursor.execute("""
                MERGE dbo.dim_geolocation AS target
                USING (
                    SELECT
                        country, region, city, latitude, longitude,
                        MAX(isp)                                             AS isp,
                        CONVERT(NVARCHAR(64), HASHBYTES('SHA2_256',
                            ISNULL(country, '') + '|'
                            + ISNULL(region, '') + '|'
                            + ISNULL(city, '') + '|'
                            + ISNULL(CAST(latitude AS NVARCHAR), '') + '|'
                            + ISNULL(CAST(longitude AS NVARCHAR), '')
                        ), 2)                                               AS geo_hash
                    FROM dbo.raw_enriched
                    WHERE country IS NOT NULL
                    GROUP BY country, region, city, latitude, longitude
                ) AS source
                ON target.geo_hash = source.geo_hash
                WHEN NOT MATCHED THEN
                    INSERT (geo_hash, country, region, city, latitude, longitude, isp)
                    VALUES (source.geo_hash, source.country, source.region,
                            source.city, source.latitude, source.longitude, source.isp);
            """)
            conn.commit()

            # ── dim_useragent (MERGE upsert on ua_hash) ─────────────
            # Parse raw user-agent strings from dbo.raw_enriched using user-agents library
            # (matching the pattern from the old pipeline's airflow/plugins/operators/export_dimensions.py)
            cursor.execute("""
                IF OBJECT_ID('dbo.dim_useragent') IS NULL
                BEGIN
                    CREATE TABLE dbo.dim_useragent (
                        user_agent_sk   INT IDENTITY(1,1) PRIMARY KEY,
                        ua_hash         NVARCHAR(64)   NOT NULL,
                        agent_type      NVARCHAR(50)   NULL,
                        browser_name    NVARCHAR(100)  NULL,
                        browser_version NVARCHAR(50)   NULL,
                        os              NVARCHAR(100)  NULL,
                        device_type     NVARCHAR(50)   NULL,
                        CONSTRAINT uq_dim_useragent__ua_hash
                            UNIQUE (ua_hash)
                    );

                    -- Seed -1 sentinel unknown row for FK integrity
                    SET IDENTITY_INSERT dbo.dim_useragent ON;
                    INSERT INTO dbo.dim_useragent (user_agent_sk, ua_hash, agent_type, browser_name, browser_version, os, device_type)
                    VALUES (-1, '0000000000000000000000000000000000000000000000000000000000000000', 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown');
                    SET IDENTITY_INSERT dbo.dim_useragent OFF;
                END;
            """)

            # Read distinct user_agent strings from dbo.raw_enriched
            cursor.execute("SELECT DISTINCT user_agent FROM dbo.raw_enriched WHERE user_agent IS NOT NULL AND user_agent != '-'")
            ua_rows = cursor.fetchall()

            if ua_rows:
                # Parse user-agent strings using user-agents library
                try:
                    from user_agents import parse as ua_parse
                    import hashlib
                except ImportError:
                    logger.warning("user-agents library not installed; skipping dim_useragent build")
                    ua_rows = []
                else:
                    logger.info(f"Parsing {len(ua_rows)} distinct user-agent strings...")
                    insert_data = []
                    seen_hashes = set()
                    for idx, (ua_raw,) in enumerate(ua_rows):
                        ua_str = unquote_plus(ua_raw[:1000] if ua_raw else "")
                        parsed = ua_parse(ua_str)

                        agent_type = "Crawler" if getattr(parsed, "is_bot", False) else "Browser"
                        browser_name = parsed.browser.family or "Unknown"
                        browser_version = parsed.browser.version_string or "Unknown"
                        os_name = parsed.os.family or "Unknown"

                        if getattr(parsed, "is_mobile", False):
                            device = "Mobile"
                        elif getattr(parsed, "is_tablet", False):
                            device = "Tablet"
                        elif getattr(parsed, "is_pc", False):
                            device = "Desktop"
                        elif getattr(parsed, "is_bot", False):
                            device = "Bot"
                        else:
                            device = "Other"

                        # Compute hash in Python (much faster than SQL round-trips)
                        hash_input = f"{agent_type or ''}|{browser_name or ''}|{browser_version or ''}|{os_name or ''}|{device or ''}"
                        ua_hash = hashlib.sha256(hash_input.encode('utf-8')).hexdigest()

                        # Dedup after URL-unescaping: two differently-escaped raw strings may
                        # unquote_plus to the same UA, producing the same hash
                        if ua_hash in seen_hashes:
                            continue
                        seen_hashes.add(ua_hash)

                        insert_data.append((ua_hash, agent_type, browser_name, browser_version, os_name, device))

                        # Log progress every 500 UAs
                        if (idx + 1) % 500 == 0:
                            logger.info(f"Parsed {idx + 1}/{len(ua_rows)} user-agents...")

                    # Batch insert with MERGE for idempotency (batched VALUES for performance)
                    if insert_data:
                        logger.info(f"Inserting {len(insert_data)} rows into dim_useragent...")
                        batch_size = 300  # Stay under SQL Server's 2100-param limit (6 params/row)
                        for batch_start in range(0, len(insert_data), batch_size):
                            batch = insert_data[batch_start:batch_start + batch_size]
                            placeholders = ",".join(["(?,?,?,?,?,?)"] * len(batch))
                            params = tuple(v for row in batch for v in row)
                            cursor.execute(f"""
                                MERGE dbo.dim_useragent AS target
                                USING (VALUES {placeholders}) AS source (ua_hash, agent_type, browser_name, browser_version, os, device_type)
                                ON target.ua_hash = source.ua_hash
                                WHEN NOT MATCHED THEN
                                    INSERT (ua_hash, agent_type, browser_name, browser_version, os, device_type)
                                    VALUES (source.ua_hash, source.agent_type, source.browser_name,
                                            source.browser_version, source.os, source.device_type);
                            """, params)
                        logger.info(f"Inserted {len(insert_data)} rows into dim_useragent")
            else:
                logger.info("No user-agent strings found in dbo.raw_enriched; skipping dim_useragent build")

            conn.commit()

        logger.info(
            "Dimension tables dim_geolocation and dim_useragent built successfully from Azure SQL."
        )

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


# ── DAG definition ─────────────────────────────────────────────────────────
dag = DAG(
    dag_id="w3c_spark_ingestion_azure",
    schedule="0 2 * * *",  # Daily at 2:00 AM UTC
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
    dag=dag,
)

# ════════════════════════════════════════════════════════════════════════════
# TASK 2: Build Airflow-Managed Dimensions from Azure SQL
# ════════════════════════════════════════════════════════════════════════════

export_dimensions = PythonOperator(
    task_id="export_dimensions",
    python_callable=_export_dimensions,
    outlets=[AZURE_WAREHOUSE_LOADED],
    dag=dag,
)

# ════════════════════════════════════════════════════════════════════════════
# Dependencies
# ════════════════════════════════════════════════════════════════════════════

bronze_silver_jdbc_pipeline >> export_dimensions
