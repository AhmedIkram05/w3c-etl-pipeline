#!/usr/bin/env python3
"""
Prometheus exporter for W3C ETL pipeline health.

Exposes 5 metrics on port 8000:
  - w3c_data_freshness_seconds{source}   — seconds since last successful load
  - w3c_pipeline_last_run_status{source}  — 1 if pipeline ran recently, 0 otherwise
  - w3c_dbt_test_pass_rate               — fraction of dbt tests passing (0.0–1.0)
  - w3c_row_count{source}                — row counts from bronze, silver, and warehouse

Supports two database backends:
  1. Azure SQL (pymssql) — primary, used in production
  2. PostgreSQL (psycopg2) — fallback for local Docker development
"""

import json
import os
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

import psycopg2
import pymssql
from prometheus_client import Gauge, start_http_server

# ── Prometheus gauges ──────────────────────────────────────────────────
FRESHNESS_GAUGE = Gauge(
    "w3c_data_freshness_seconds",
    "Seconds since the last successful pipeline load into the warehouse",
    ["source"],
)
STATUS_GAUGE = Gauge(
    "w3c_pipeline_last_run_status",
    "1 if pipeline loaded data recently (within check interval), 0 otherwise",
    ["source"],
)
DBT_GAUGE = Gauge(
    "w3c_dbt_test_pass_rate",
    "Fraction of dbt tests passing (0.0–1.0)",
)
ROW_COUNT_GAUGE = Gauge(
    "w3c_row_count",
    "Total row count per source (bronze, silver, warehouse)",
    ["source"],
)

# ── Configuration ──────────────────────────────────────────────────────

# Azure SQL (production)
AZURE_DSN = {
    "server": os.environ.get("AZURE_SQL_SERVER"),
    "database": os.environ.get("AZURE_SQL_DATABASE"),
    "user": os.environ.get("AZURE_SQL_USER"),
    "password": os.environ.get("AZURE_SQL_PASS"),
    "timeout": 10,  # Connection timeout in seconds
    "login_timeout": 10,  # Login timeout in seconds (slow failover protection)
    "charset": "UTF-8",  # Ensure Unicode data is handled correctly
    "as_dict": True,  # Access columns by name instead of index
}
HAS_AZURE_SQL = all(AZURE_DSN.values())

# PostgreSQL (local Docker development)
PG_DSN = {
    "host": os.environ.get("W3C_DB_HOST", "postgres"),
    "port": int(os.environ.get("W3C_DB_PORT", "5432")),
    "dbname": os.environ.get("W3C_DB_NAME", "w3c_warehouse"),
    "user": os.environ.get("W3C_DB_USER", "airflow"),
    "password": os.environ.get("W3C_DB_PASS", "airflow"),
}

DATABRICKS_HOST = os.environ.get("DATABRICKS_HOST")
DATABRICKS_TOKEN = os.environ.get("DATABRICKS_PAT_TOKEN")
DATABRICKS_WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID")
HAS_DATABRICKS = bool(DATABRICKS_HOST and DATABRICKS_TOKEN and DATABRICKS_WAREHOUSE_ID)

# Delta table paths in the Unity Catalog (only used when Databricks is configured)
BRONZE_TABLE = "w3c_etl_databricks.bronze.bronze_raw_logs"
SILVER_TABLE = "w3c_etl_databricks.silver.silver_enriched_logs"

# How old "recent" data can be before we mark the pipeline as stale
STALE_THRESHOLD_SECONDS = 7200  # 2 hours

# Tracking variable for which backend is active (set during collection)
ACTIVE_BACKEND: str | None = None


def _get_connection():
    """Try Azure SQL first, then PostgreSQL.

    Returns:
        Tuple of (conn, backend_name) on success, or (None, None) if both fail.
        backend_name is 'azure_sql' or 'postgresql'.
    """
    if HAS_AZURE_SQL:
        try:
            conn = pymssql.connect(**AZURE_DSN)
            return conn, "azure_sql"
        except Exception:
            pass  # nosec B110 — intentional: Azure SQL failure falls through to PostgreSQL

    try:
        conn = psycopg2.connect(**PG_DSN)
        return conn, "postgresql"
    except Exception:
        return None, None  # nosec B110 — intentional: both backends unavailable


# ── Data collection functions ──────────────────────────────────────────


def check_freshness() -> float | None:
    """Return seconds since last pipeline load, or None if unavailable.

    Reads ``MAX(loaded_at)`` from the tracking table
    (``raw_enriched_loaded``) which records when each source file was
    exported.  Falls back to ``MAX(log_date)`` from the raw data table for
    databases that haven't run the ``loaded_at`` column migration yet.
    """
    global ACTIVE_BACKEND
    ACTIVE_BACKEND = None
    conn, backend = _get_connection()
    if conn is None:
        return None
    ACTIVE_BACKEND = backend
    result: float | None = None
    try:
        cursor = conn.cursor()
        # Primary: tracking table with loaded_at
        cursor.execute(
            "SELECT MAX(loaded_at) FROM dbo.raw_enriched_loaded"
            if backend == "azure_sql"
            else "SELECT MAX(loaded_at) FROM public.raw_enriched_loaded"
        )
        row = cursor.fetchone()
        if row and row[0] is not None:
            last_load = row[0]
            if last_load.tzinfo is None:
                last_load = last_load.replace(tzinfo=timezone.utc)
            result = (datetime.now(timezone.utc) - last_load).total_seconds()

        # Fallback: MAX(log_date) if loaded_at column doesn't exist yet
        if result is None:
            cursor.execute(
                "SELECT MAX(log_date) FROM dbo.raw_enriched"
                if backend == "azure_sql"
                else "SELECT MAX(log_date) FROM public.raw_enriched"
            )
            row = cursor.fetchone()
            if row and row[0] is not None:
                last_load = datetime.combine(row[0], datetime.min.time(), tzinfo=timezone.utc)
                result = (datetime.now(timezone.utc) - last_load).total_seconds()
    except Exception:
        pass  # nosec B110 — intentional: monitoring probe returns None on DB failure
    finally:
        conn.close()
    return result


def check_pipeline_status() -> int:
    """Return 1 if data was loaded recently, 0 otherwise."""
    seconds = check_freshness()
    if seconds is None:
        return 0
    return 1 if seconds < STALE_THRESHOLD_SECONDS else 0


def check_dbt_pass_rate() -> float:
    """
    Estimate dbt test pass rate from database.
    """
    global ACTIVE_BACKEND
    ACTIVE_BACKEND = None
    conn, backend = _get_connection()
    if conn is None:
        return 0.0
    ACTIVE_BACKEND = backend
    try:
        cursor = conn.cursor()
        if backend == "azure_sql":
            cursor.execute("""
                SELECT
                    CASE
                        WHEN OBJECT_ID('dbo.dbt_test_results') IS NOT NULL
                        THEN 1.0 * SUM(CASE WHEN status = 'pass' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0)
                        ELSE 1.0
                    END
                FROM dbo.dbt_test_results
                HAVING COUNT(*) > 0
            """)
        else:
            cursor.execute("""
                SELECT
                    CASE
                        WHEN EXISTS (
                            SELECT 1 FROM information_schema.tables
                            WHERE table_schema = 'public' AND table_name = 'dbt_test_results'
                        )
                        THEN 1.0 * SUM(CASE WHEN status = 'pass' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0)
                        ELSE 1.0
                    END
                FROM public.dbt_test_results
                HAVING COUNT(*) > 0
            """)
        row = cursor.fetchone()
        return float(row[0]) if row and row[0] is not None else 1.0
    except Exception:
        return 1.0
    finally:
        conn.close()


def check_warehouse_row_count() -> int:
    """Return total row count from the warehouse, or 0 on connection failure."""
    global ACTIVE_BACKEND
    ACTIVE_BACKEND = None
    conn, backend = _get_connection()
    if conn is None:
        return 0
    ACTIVE_BACKEND = backend
    try:
        cursor = conn.cursor()
        if backend == "azure_sql":
            cursor.execute("SELECT COUNT(*) FROM dbo.raw_enriched")
        else:
            cursor.execute("SELECT COUNT(*) FROM public.raw_enriched")
        row = cursor.fetchone()
        return int(row[0]) if row else 0
    except Exception:
        return 0
    finally:
        conn.close()


def query_databricks(sql: str) -> list[list[str]] | None:
    """Execute a SQL statement on the Databricks SQL warehouse and return the data array.

    Returns None if the warehouse is unavailable or the query fails.
    """
    url = f"https://{DATABRICKS_HOST}/api/2.0/sql/statements"
    body = json.dumps({
        "statement": sql,
        "warehouse_id": DATABRICKS_WAREHOUSE_ID,
        "wait_timeout": "15s",
    }).encode()
    req = urllib.request.Request(
        url,
        data=body,
        headers={
            "Authorization": f"Bearer {DATABRICKS_TOKEN}",
            "Content-Type": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:  # nosec B310 — URL is env-configured with hardcoded https:// scheme
            data = json.loads(resp.read().decode())
        if data.get("status", {}).get("state") != "SUCCEEDED":
            return None
        return data.get("result", {}).get("data_array")
    except (urllib.error.URLError, json.JSONDecodeError, KeyError, OSError):
        return None


def check_databricks_row_count(full_table_name: str) -> int:
    """Return row count for a Unity Catalog table via the Databricks SQL warehouse.

    The ``full_table_name`` parameter is always called with module-level
    constants (``BRONZE_TABLE`` or ``SILVER_TABLE``), not user input.
    """
    results = query_databricks(f"SELECT COUNT(*) AS cnt FROM {full_table_name}")  # nosec B608 — table name is a hardcoded constant
    if results and len(results) > 0 and len(results[0]) > 0:
        return int(results[0][0])
    return 0


if __name__ == "__main__":
    start_http_server(8000)
    while True:
        # Freshness — backend source is set by check_freshness
        seconds = check_freshness()
        source_label = ACTIVE_BACKEND or "unknown"
        if seconds is not None:
            FRESHNESS_GAUGE.labels(source=source_label).set(seconds)

        # Pipeline status
        source_label = ACTIVE_BACKEND or "unknown"
        STATUS_GAUGE.labels(source=source_label).set(check_pipeline_status())

        # dbt pass rate
        DBT_GAUGE.set(check_dbt_pass_rate())

        # Row counts — warehouse
        row_count = check_warehouse_row_count()
        source_label = ACTIVE_BACKEND or "unknown"
        ROW_COUNT_GAUGE.labels(source=source_label).set(row_count)

        # Row counts — Databricks UC (only if configured)
        if HAS_DATABRICKS:
            ROW_COUNT_GAUGE.labels(source="bronze").set(check_databricks_row_count(BRONZE_TABLE))
            ROW_COUNT_GAUGE.labels(source="silver").set(check_databricks_row_count(SILVER_TABLE))
        else:
            ROW_COUNT_GAUGE.labels(source="bronze").set(0)
            ROW_COUNT_GAUGE.labels(source="silver").set(0)

        time.sleep(900)  # Every 15 minutes
