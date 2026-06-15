"""
W3C dbt Marts DAG — Dataset-triggered dbt pipeline.

Runs after the Spark ingestion DAG completes and publishes dbt models
(staging → fact → marts) to the PostgreSQL warehouse, then exports
star-schema tables as CSV files for Power BI consumption.

Schedule
--------
Dataset-triggered by ``Dataset("postgres://postgres:5432/w3c_warehouse/public/raw_enriched_loaded")``,
which is emitted by the ``spark_ingestion`` DAG when Gold-level Delta tables have
been written to PostgreSQL.
"""

from __future__ import annotations

import datetime as dt
import os

from airflow.datasets import Dataset
from airflow.operators.bash import BashOperator

from airflow import DAG

# ── Paths ────────────────────────────────────────────────────────────
DBT_PROJECT_DIR = "/opt/airflow/dbt/w3c"
DBT_PROFILES_DIR = "/opt/airflow/dbt"
STAR_SCHEMA_DIR = "/opt/airflow/data/Star-Schema"

# ── Tables to export ─────────────────────────────────────────────────
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
    "public.dim_geolocation",
    "public.dim_useragent",
]

ALL_TABLES = STAGING_TABLES + MART_TABLES + PUBLIC_TABLES

# Build the export script dynamically from the table list.
EXPORT_SCRIPT = f"mkdir -p {STAR_SCHEMA_DIR}\n" + "\n".join(
    f"psql -h postgres -U airflow -d w3c_warehouse -c \"\\copy (SELECT * FROM {table}) TO '{STAR_SCHEMA_DIR}/{table}.csv' WITH CSV HEADER\""  # nosec B608 — table names are module-level constants (ALL_TABLES)
    for table in ALL_TABLES
)

# ── DAG definition ───────────────────────────────────────────────────

dag = DAG(
    dag_id="w3c_dbt_marts",
    schedule=[Dataset("postgres://postgres:5432/w3c_warehouse/public/raw_enriched_loaded")],
    start_date=dt.datetime(2026, 3, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "w3c-team",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": dt.timedelta(minutes=2),
    },
    description="Dataset-triggered dbt pipeline: staging → fact → marts → CSV export",
    tags=["w3c", "dbt", "marts", "dataset-triggered"],
)

# ── Tasks ────────────────────────────────────────────────────────────

dbt_deps = BashOperator(
    task_id="dbt_deps",
    bash_command=f"""
if [ ! -d "{DBT_PROJECT_DIR}/dbt_packages" ]; then
    dbt deps --project-dir {DBT_PROJECT_DIR}
else
    echo "dbt_packages already exists, skipping dbt deps"
fi
""",
    dag=dag,
)

dbt_run = BashOperator(
    task_id="dbt_run",
    bash_command=f"dbt run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}",
    dag=dag,
)

dbt_test = BashOperator(
    task_id="dbt_test",
    bash_command=f"dbt test --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}",
    dag=dag,
)

dbt_docs = BashOperator(
    task_id="dbt_docs",
    bash_command=f"dbt docs generate --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}",
    dag=dag,
)

export_csv = BashOperator(
    task_id="export_csv",
    bash_command=EXPORT_SCRIPT,
    env={"PGPASSWORD": os.environ.get("W3C_DB_PASS", "airflow")},
    dag=dag,
)

# ── Dependencies ─────────────────────────────────────────────────────
dbt_deps >> dbt_run >> dbt_test >> dbt_docs >> export_csv
