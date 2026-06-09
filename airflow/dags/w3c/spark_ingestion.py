"""
W3C Spark Ingestion DAG — Bronze / Silver / Export for Combined Architecture.

This DAG is the first stage of the ETL pipeline. It runs the PySpark +
Delta Lake medallion pipeline (bronze → silver), exports the
enriched data to PostgreSQL, then builds Airflow-managed dimension tables
that require external APIs or Python libraries.

The output ``Dataset("postgres://postgres:5432/w3c_warehouse/public/raw_enriched_loaded")``
triggers the downstream dbt DAG for analytics-layer transformations.

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

GeoLite2 Setup
--------------
The ``silver_enrichment`` task performs Geo-IP lookups against the MaxMind
GeoLite2-City database. By default it expects the ``GeoLite2-City.mmdb`` file
at ``/opt/spark/data/GeoLite2-City.mmdb`` (inside the Spark container).

Override the path by setting the ``GEOIP_DB_PATH`` environment variable in
the Airflow worker / Spark container before running the DAG.

To obtain the ``.mmdb`` file (free registration required):
  1. Sign up at https://www.maxmind.com/en/geolite2/signup
  2. Download the **GeoLite2 City** database (binary ``.mmdb`` format).
  3. Mount/copy ``GeoLite2-City.mmdb`` into the Spark container at the path
     referenced by ``GEOIP_DB_PATH`` (or the default location).

If the file is missing at runtime, the silver task will log a warning and
geo fields (``country``, ``region``, ``city``, ``latitude``, ``longitude``,
``isp``) will default to ``"Unknown"`` / ``None``. The DAG will still
complete successfully — downstream analytics will simply lack geo data.
"""

from __future__ import annotations

import datetime as dt
import os
import zipfile

from airflow.datasets import Dataset
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from operators.export_dimensions import export_dimensions as _export_dimensions

from airflow import DAG

# ── Dataset outlet — signals downstream dbt DAG ────────────────────────────
WAREHOUSE_LOADED = Dataset("postgres://postgres:5432/w3c_warehouse/public/raw_enriched_loaded")

# ── Paths (inside Airflow worker containers) ───────────────────────────────
SPARK_JOBS_DIR = "/opt/airflow/spark/jobs"
DELTA_DIR = "/opt/spark/delta"
LOG_FILES_DIR = "/opt/spark/logfiles"

# ── Package ``utils/`` for Spark executors ─────────────────────────────────
# Spark workers do not have /opt/spark/jobs on their Python path, so the
# ``from utils.schemas import ...`` / ``from utils.w3c_parser import ...``
# imports in the Spark jobs would fail when the UDF is evaluated on a worker.
# We rebuild ``utils.zip`` (and ship it via --py-files) whenever any source
# file inside ``utils/`` is newer than the existing zip.
_UTILS_DIR = os.path.join(SPARK_JOBS_DIR, "utils")
_UTILS_ZIP = os.path.join(SPARK_JOBS_DIR, "utils.zip")


def _build_utils_zip() -> str:
    """Return the path to ``utils.zip``, rebuilding it if sources have changed.

    The zip's internal layout keeps the ``utils/`` package root, so when Spark
    extracts it on each worker, ``import utils.schemas`` resolves correctly.
    """
    src_mtimes = [
        os.path.getmtime(os.path.join(root, fname))
        for root, _, files in os.walk(_UTILS_DIR)
        for fname in files
        if fname.endswith(".py")
    ]
    if src_mtimes and os.path.exists(_UTILS_ZIP):
        if os.path.getmtime(_UTILS_ZIP) >= max(src_mtimes):
            return _UTILS_ZIP

    with zipfile.ZipFile(_UTILS_ZIP, "w", zipfile.ZIP_DEFLATED) as zf:
        for root, _, files in os.walk(_UTILS_DIR):
            for fname in files:
                if not fname.endswith(".py"):
                    continue
                src_path = os.path.join(root, fname)
                # arcname = "utils/geoip.py", "utils/__init__.py", etc.
                arc = os.path.relpath(src_path, SPARK_JOBS_DIR)
                zf.write(src_path, arcname=arc)
    return _UTILS_ZIP


UTILS_PY_FILES = _build_utils_zip()

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
    py_files=UTILS_PY_FILES,
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
    py_files=UTILS_PY_FILES,
    application_args=[
        "--delta-dir",
        DELTA_DIR,
        "--geolite2-db",
        os.environ.get("GEOIP_DB_PATH", "/opt/spark/data/GeoLite2-City.mmdb"),
        "--geolite2-asn-db",
        os.environ.get("GEOIP_ASN_DB_PATH", "/opt/spark/data/GeoLite2-ASN.mmdb"),
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
    py_files=UTILS_PY_FILES,
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
