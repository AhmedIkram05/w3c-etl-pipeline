"""
W3C Spark Ingestion DAG — Bronze / Silver / Export for Combined Architecture.

This DAG is the first stage of the ETL pipeline. It runs the PySpark +
Delta Lake medallion pipeline (bronze → silver), exports the
enriched data to PostgreSQL, then builds Airflow-managed dimension tables
that require external APIs or Python libraries.

The output ``Dataset("postgres://warehouse/loaded")`` triggers the downstream
dbt DAG for analytics-layer transformations.

Schedule
--------
Saturdays at 6:00 AM — after the Spark worker pool has finished the prior
day's load.

Spark Cluster
-------------
- Master: ``spark://spark-master:7077``
- Delta Lake 4.0.1 via ``spark.jars.packages`` (resolved by Ivy on the
  Airflow worker at runtime).
- PySpark 4.0.2 on the Airflow worker for RPC compatibility.
"""

from __future__ import annotations

import datetime as dt
import os

from airflow.datasets import Dataset
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from operators.export_dimensions import export_dimensions as _export_dimensions

from airflow import DAG

# ── Dataset outlet — signals downstream dbt DAG ────────────────────────────
WAREHOUSE_LOADED = Dataset("postgres://warehouse/loaded")

# ── Paths (inside Airflow worker containers) ───────────────────────────────
SPARK_JOBS_DIR = "/opt/airflow/spark/jobs"
DELTA_DIR = "/opt/spark/delta"
LOG_FILES_DIR = "/opt/spark/logfiles"

# ── Spark configuration (mirrors w3c-spark-dag.py) ────────────────────────
_SPARK_CONF = {
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.sources.partitionOverwriteMode": "dynamic",
    "spark.databricks.delta.retentionDurationCheck.enabled": "false",
    # Disable Python version check: Airflow driver uses 3.12, Spark workers use 3.10
    "spark.python.versionCheck.enabled": "false",
    # Delta Lake JAR — resolved automatically from Maven Central at runtime
    "spark.jars.packages": "io.delta:delta-spark_2.13:4.0.1,org.postgresql:postgresql:42.7.4",
    # Ivy cache in a writable location (airflow user home)
    "spark.jars.ivy": "/home/airflow/.ivy2",
}

# ── Spark resources ────────────────────────────────────────────────────────
_EXECUTOR_CORES = 1
_TOTAL_EXECUTOR_CORES = 2
_EXECUTOR_MEMORY = "2g"
_DRIVER_MEMORY = "1g"

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

# ── DAG definition (standard style, not @dag decorator) ────────────────────
dag = DAG(
    dag_id="w3c_spark_ingestion",
    schedule="0 6 * * 6",  # Saturday 6 AM
    start_date=dt.datetime(2026, 3, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    description="Combined pipeline: Spark Bronze → Silver → PostgreSQL export → Airflow dimensions",
    tags=["w3c", "spark", "medallion", "combined"],
)

# ══════════════════════════════════════════════════════════════════════════
# TASK 1: Bronze Ingestion
# ══════════════════════════════════════════════════════════════════════════

bronze_ingestion = SparkSubmitOperator(
    task_id="bronze_ingestion",
    application=f"{SPARK_JOBS_DIR}/bronze_ingestion.py",
    conn_id="spark_default",
    name="w3c_bronze_ingestion",
    verbose=True,
    driver_memory=_DRIVER_MEMORY,
    executor_memory=_EXECUTOR_MEMORY,
    executor_cores=_EXECUTOR_CORES,
    total_executor_cores=_TOTAL_EXECUTOR_CORES,
    conf=_SPARK_CONF,
    application_args=[
        "--log-files-dir",
        LOG_FILES_DIR,
        "--delta-dir",
        DELTA_DIR,
    ],
    dag=dag,
)

# ══════════════════════════════════════════════════════════════════════════
# TASK 2: Silver Enrichment
# ══════════════════════════════════════════════════════════════════════════

silver_enrichment = SparkSubmitOperator(
    task_id="silver_enrichment",
    application=f"{SPARK_JOBS_DIR}/silver_enrichment.py",
    conn_id="spark_default",
    name="w3c_silver_enrichment",
    verbose=True,
    driver_memory=_DRIVER_MEMORY,
    executor_memory=_EXECUTOR_MEMORY,
    executor_cores=_EXECUTOR_CORES,
    total_executor_cores=_TOTAL_EXECUTOR_CORES,
    conf=_SPARK_CONF,
    application_args=[
        "--delta-dir",
        DELTA_DIR,
        "--geolite2-db",
        "/opt/spark/data/GeoLite2-City.mmdb",
    ],
    dag=dag,
)

# ══════════════════════════════════════════════════════════════════════════
# TASK 3: Export Enriched Data to PostgreSQL Warehouse
# ══════════════════════════════════════════════════════════════════════════

export_warehouse = SparkSubmitOperator(
    task_id="export_warehouse",
    application=f"{SPARK_JOBS_DIR}/export_warehouse.py",
    conn_id="spark_default",
    name="w3c_export_warehouse",
    verbose=True,
    driver_memory=_DRIVER_MEMORY,
    executor_memory=_EXECUTOR_MEMORY,
    executor_cores=_EXECUTOR_CORES,
    total_executor_cores=_TOTAL_EXECUTOR_CORES,
    conf=_SPARK_CONF,
    application_args=[
        "--delta-dir",
        DELTA_DIR,
        "--jdbc-url",
        "jdbc:postgresql://postgres:5432/w3c_warehouse",
        "--jdbc-user",
        os.environ.get("W3C_DB_USER", "airflow"),
        "--jdbc-password",
        os.environ.get("W3C_DB_PASS", "airflow"),
        "--jdbc-driver",
        "org.postgresql.Driver",
    ],
    dag=dag,
)

# ══════════════════════════════════════════════════════════════════════════
# TASK 4: Export Airflow-Managed Dimensions
# ══════════════════════════════════════════════════════════════════════════

export_dimensions = PythonOperator(
    task_id="export_dimensions",
    python_callable=_export_dimensions,
    outlets=[WAREHOUSE_LOADED],
    dag=dag,
)

# ══════════════════════════════════════════════════════════════════════════
# Dependencies
# ══════════════════════════════════════════════════════════════════════════

bronze_ingestion >> silver_enrichment >> export_warehouse >> export_dimensions
