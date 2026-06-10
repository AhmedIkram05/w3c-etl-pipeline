"""
Export dbt Tables from Azure SQL to CSV Files for Power BI.

Reads all 18 dbt model tables (10 staging + 6 marts + 2 public dbo) from
Azure SQL via ``pyodbc`` and writes them as CSV files to
``/opt/airflow/data/Star-Schema/`` for Power BI consumption.

The exact table list and file naming conventions match the PostgreSQL-based
``dbt_marts.py`` DAG's ``export_csv`` task, ensuring Power BI semantic model
compatibility.

Idempotent — every run overwrites the CSV files with current data.
Gracefully degrades if Azure SQL credentials are missing or pyodbc is
unavailable.
"""

from __future__ import annotations

import logging
import os

logger = logging.getLogger(__name__)

STAR_SCHEMA_DIR = "/opt/airflow/data/Star-Schema"

STAGING_TABLES = [
    "dbt_staging.fact_webrequest",
    "dbt_staging.dim_date",
    "dbt_staging.dim_time",
    "dbt_staging.dim_page",
    "dbt_staging.dim_status",
    "dbt_staging.dim_referrer",
    "dbt_staging.dim_method",
    "dbt_staging.dim_visitortype",
    "dbt_staging.dim_visit_buckets",
    "dbt_staging.crawler_ips",
]

MART_TABLES = [
    "dbt_marts.mart_page_performance",
    "dbt_marts.mart_daily_aggregates",
    "dbt_marts.mart_crawler_analysis",
    "dbt_marts.mart_browser_analysis",
    "dbt_marts.mart_timeofday_analysis",
    "dbt_marts.mart_country_browser_share",
]

PUBLIC_TABLES = [
    "dbo.dim_geolocation",
    "dbo.dim_useragent",
]

ALL_TABLES = STAGING_TABLES + MART_TABLES + PUBLIC_TABLES


def export_csv_azure(**context) -> None:
    """Export dbt mart and staging tables from Azure SQL to CSV files.

    Reads from Azure SQL ``dbt_staging.*``, ``dbt_marts.*``, and
    ``dbo.dim_*`` tables, writing CSVs to ``/opt/airflow/data/Star-Schema/``.
    Public tables (``dbo.*``) are saved with a ``public.`` filename prefix
    to match the Power BI contract established by the original PostgreSQL
    pipeline.

    Environment variables
    ---------------------
    AZURE_SQL_SERVER : str
        Azure SQL server FQDN.
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
            "AZURE_SQL_USER, AZURE_SQL_PASS env vars). Skipping CSV export."
        )
        return

    try:
        import pyodbc
    except ImportError:
        logger.warning(
            "pyodbc not installed — cannot export CSVs from Azure SQL. "
            "Install pyodbc, or run inside the Airflow container."
        )
        return

    conn_str = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={password};"
        f"Encrypt=yes;TrustServerCertificate=no;"
    )

    os.makedirs(STAR_SCHEMA_DIR, exist_ok=True)

    try:
        with pyodbc.connect(conn_str, timeout=30) as conn:
            for table in STAGING_TABLES + MART_TABLES:
                _export_table(conn, table, f"{STAR_SCHEMA_DIR}/{table}.csv")

            # Public tables (dbo.*) use 'public.' prefix to match Power BI contract
            for table in PUBLIC_TABLES:
                file_name = table.replace("dbo.", "public.")
                _export_table(conn, table, f"{STAR_SCHEMA_DIR}/{file_name}.csv")

        logger.info(
            f"CSV export complete: {len(ALL_TABLES)} tables exported "
            f"to {STAR_SCHEMA_DIR}"
        )
    except pyodbc.Error:
        logger.exception("Azure SQL error during CSV export")
        raise


def _export_table(conn, table_name: str, csv_path: str) -> None:
    """Read a table from Azure SQL and write it to a CSV file.

    Uses ``pandas.read_sql`` for efficient column-aware export with
    automatic type coercion. Overwrites any existing CSV at *csv_path*.
    """
    import pandas as pd

    try:
        df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
        df.to_csv(csv_path, index=False)
        logger.info(
            f"Exported {table_name} -> {csv_path} ({len(df)} rows)"
        )
    except Exception as exc:
        logger.warning(
            f"Failed to export {table_name}: {exc}"
        )
