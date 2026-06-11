"""
W3C dbt Marts Azure DAG — Dataset-Triggered dbt Pipeline on Azure SQL.

Runs after the ``w3c_spark_ingestion_azure`` DAG completes its
``export_dimensions`` task (which fires
``Dataset("mssql://azure-sql/dbo/raw_enriched_loaded")``).

This DAG handles the **transformation layer**:
 - dbt source freshness check
 - dbt model execution (staging + marts)
 - dbt data quality tests
 - dbt docs generation
 - CSV export for Power BI
 - dbt docs artifact sync

Architecture
------------
dbt runs on Databricks via ``DatabricksSubmitRunOperator`` submitting
notebook tasks to serverless compute. Each notebook bootstraps its
``dbt-core==1.8.9`` and ``dbt-sqlserver==1.8.4`` dependencies via pip
at runtime — serverless Databricks rejects the task-level ``libraries``
field (must be specified in the job environment instead).

The CSV export and docs sync run on the Airflow worker via
``PythonOperator``, reading from Azure SQL (pyodbc) and Azure Blob
Storage respectively.

Decoupled from DAG 1
--------------------
This DAG is intentionally decoupled from the ingestion DAG by using
Airflow Dataset triggers. Ingestion and transformation can be developed,
tested, and monitored independently.

Schedule
--------
Dataset-triggered — no cron schedule. Runs whenever the Dataset outlet
``mssql://azure-sql/dbo/raw_enriched_loaded`` is updated by the
``w3c_spark_ingestion_azure`` DAG.
"""

from __future__ import annotations

import datetime as dt

from airflow.datasets import Dataset
from airflow.operators.python import PythonOperator
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from operators.export_csv_azure import export_csv_azure
from operators.export_dbt_docs_azure import export_dbt_docs_to_airflow

from airflow import DAG

# ── Dataset inlet — triggered by spark_ingestion_azure DAG ─────────────────
# This must match the Dataset outlet fired by spark_ingestion_azure.py's
# export_dimensions task: Dataset("mssql://azure-sql/dbo/raw_enriched_loaded")
AZURE_WAREHOUSE_LOADED = Dataset("mssql://azure-sql/dbo/raw_enriched_loaded")

# ── Outlets for downstream monitoring / future DAGs ─────────────────────────
DBT_DOCS_READY = Dataset("azure://w3c-etl/dbt_docs_ready")
CSV_EXPORTS_READY = Dataset("azure://w3c-etl/csv_exports_ready")

# ── Repo path (consistent with existing Databricks notebooks) ──────────────
_REPO_ROOT = "/Repos/w3c-etl-pipeline"

# ── Default arguments ──────────────────────────────────────────────────────
default_args = {
    "owner": "w3c-team",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": dt.timedelta(minutes=2),
}


def _build_dbt_task(task_key: str, notebook_name: str) -> list[dict]:
    """Build a ``tasks`` list (multi-task format) for a single dbt notebook.

    Serverless Databricks requires the ``tasks`` array format even for
    single-notebook submissions. Each task wraps a ``notebook_task``
    with the notebook path relative to the Databricks Repos mirror.
    """
    return [
        {
            "task_key": task_key,
            "notebook_task": {
                "notebook_path": f"{_REPO_ROOT}/airflow/spark/databricks/{notebook_name}",
            },
        }
    ]


# ── DAG definition ─────────────────────────────────────────────────────────
dag = DAG(
    dag_id="w3c_dbt_marts_azure",
    schedule=[AZURE_WAREHOUSE_LOADED],
    start_date=dt.datetime(2026, 3, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    description="Dataset-triggered dbt pipeline: source freshness → run → test → docs → CSV export (Azure SQL)",
    tags=["w3c", "dbt", "marts", "azure", "dataset-triggered"],
)

# ════════════════════════════════════════════════════════════════════════════
# TASK 1: dbt Source Freshness
# ════════════════════════════════════════════════════════════════════════════
dbt_source_freshness = DatabricksSubmitRunOperator(
    task_id="dbt_source_freshness",
    databricks_conn_id="databricks_default",
    tasks=_build_dbt_task("dbt_source_freshness", "dbt_freshness.py"),
    dag=dag,
)

# ════════════════════════════════════════════════════════════════════════════
# TASK 2: dbt Run
# ════════════════════════════════════════════════════════════════════════════
dbt_run = DatabricksSubmitRunOperator(
    task_id="dbt_run",
    databricks_conn_id="databricks_default",
    tasks=_build_dbt_task("dbt_run", "dbt_run.py"),
    dag=dag,
)

# ════════════════════════════════════════════════════════════════════════════
# TASK 3: dbt Test
# ════════════════════════════════════════════════════════════════════════════
dbt_test = DatabricksSubmitRunOperator(
    task_id="dbt_test",
    databricks_conn_id="databricks_default",
    tasks=_build_dbt_task("dbt_test", "dbt_test.py"),
    dag=dag,
)

# ════════════════════════════════════════════════════════════════════════════
# TASK 4: dbt Docs Generate
# ════════════════════════════════════════════════════════════════════════════
dbt_docs = DatabricksSubmitRunOperator(
    task_id="dbt_docs",
    databricks_conn_id="databricks_default",
    tasks=_build_dbt_task("dbt_docs", "dbt_docs.py"),
    dag=dag,
)

# ════════════════════════════════════════════════════════════════════════════
# TASK 5: Export dbt Docs to Airflow
# ════════════════════════════════════════════════════════════════════════════
export_dbt_docs = PythonOperator(
    task_id="export_dbt_docs",
    python_callable=export_dbt_docs_to_airflow,
    outlets=[DBT_DOCS_READY],
    dag=dag,
)

# ════════════════════════════════════════════════════════════════════════════
# TASK 6: Export Power BI CSV Files from Azure SQL
# ════════════════════════════════════════════════════════════════════════════
export_csv = PythonOperator(
    task_id="export_csv",
    python_callable=export_csv_azure,
    outlets=[CSV_EXPORTS_READY],
    dag=dag,
)

# ════════════════════════════════════════════════════════════════════════════
# Dependencies
# ════════════════════════════════════════════════════════════════════════════
dbt_source_freshness >> dbt_run >> dbt_test
dbt_test >> export_csv
dbt_test >> dbt_docs >> export_dbt_docs
