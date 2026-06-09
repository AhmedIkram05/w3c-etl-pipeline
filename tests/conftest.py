"""
PySpark test fixtures for the W3C ETL medallion pipeline.

Provides a shared SparkSession for integration tests that need to
exercise PySpark UDFs and DataFrame operations.

Also adds necessary project paths to sys.path so all test modules
can import from ``airflow.spark.jobs``, ``airflow.plugins``, and
``airflow.dags`` using fully-qualified module names.
"""

import os
import sys

import pytest

# ── Add project source paths to sys.path ──────────────────────────────
# This allows test files to import using fully-qualified names like:
#   from utils.transformations import page_category
#   from plugins.operators.export_dimensions import _parse_user_agent

_PROJECT_ROOT = os.path.join(os.path.dirname(__file__), "..")
_AIRFLOW_DIR = os.path.join(_PROJECT_ROOT, "airflow")

# Spark jobs directory (for utils/ module imports)
_SPARK_JOBS_DIR = os.path.join(_AIRFLOW_DIR, "spark", "jobs")
if _SPARK_JOBS_DIR not in sys.path:
    sys.path.insert(0, _SPARK_JOBS_DIR)

# Airflow root directory (so "plugins.operators.export_dimensions" resolves)
if _AIRFLOW_DIR not in sys.path:
    sys.path.insert(0, _AIRFLOW_DIR)

# Airflow dags directory (so "dags.w3c.spark_ingestion" resolves)
_DAGS_DIR = os.path.join(_AIRFLOW_DIR, "dags")
if _DAGS_DIR not in sys.path:
    sys.path.insert(0, _DAGS_DIR)


def _create_spark_session():
    """Build a fresh local SparkSession for unit tests."""
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder
        .master("local[1]")
        .appName("W3C_ETL_Test")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.adaptive.enabled", "false")  # deterministic plans for tests
        .config("spark.ui.enabled", "false")  # no UI overhead
        .getOrCreate()
    )


def _spark_session_is_alive(session) -> bool:
    """Return True when the SparkSession JVM context is still active."""
    try:
        return session.sparkContext._jsc is not None  # type: ignore[attr-defined]
    except Exception:
        return False


@pytest.fixture
def spark():
    """Create a local SparkSession for each test that needs one.

    Recreates the JVM context when a prior test stopped the shared singleton
    (e.g. jdbc export E2E tests that call ``spark.stop()``).
    """
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    active = SparkSession.getActiveSession()
    if active is not None and not _spark_session_is_alive(active):
        SparkSession._instantiatedSession = None  # type: ignore[attr-defined]
        SparkSession._activeSession = None  # type: ignore[attr-defined]

    session = _create_spark_session()
    if not _spark_session_is_alive(session):
        try:
            session.stop()
        except Exception:
            pass
        SparkSession._instantiatedSession = None  # type: ignore[attr-defined]
        SparkSession._activeSession = None  # type: ignore[attr-defined]
        session = _create_spark_session()

    yield session

    if not _spark_session_is_alive(session):
        SparkSession._instantiatedSession = None  # type: ignore[attr-defined]
        SparkSession._activeSession = None  # type: ignore[attr-defined]
