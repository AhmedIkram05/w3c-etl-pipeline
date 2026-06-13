"""
PySpark test fixtures for the W3C ETL medallion pipeline.

Provides a shared SparkSession for integration tests that need to
exercise PySpark UDFs and DataFrame operations.

Also adds necessary project paths to sys.path so all test modules
can import using fully-qualified names like::

    from utils.transformations import page_category
    from plugins.operators.export_dimensions import _parse_user_agent
    from dags.w3c.spark_ingestion import _export_dimensions
"""

import os
import sys
import tempfile
import zipfile

import pytest

# ── Add project source paths to sys.path ──────────────────────────────
# CRITICAL: We do NOT add the project root (parent of ``airflow/``) to
# sys.path.  The project's ``airflow/`` directory has no ``__init__.py``,
# so Python 3.3+ treats it as a PEP 420 namespace package.  If the
# project root were on sys.path, ``import airflow`` would resolve to this
# namespace package instead of the installed ``apache-airflow`` package,
# breaking ``from airflow.models import DagBag`` with:
#
#     ModuleNotFoundError: No module named 'airflow.listeners'; 'airflow'
#     is not a package
#
# We add only the specific subdirectories that contain importable Python
# modules, which is sufficient for all test imports:

_AIRFLOW_DIR = os.path.join(os.path.abspath(os.path.dirname(__file__)), "..", "airflow")

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


# ── Cached utils.zip for PySpark workers ──────────────────────────────
# PySpark workers run in separate Python processes that do not inherit
# the driver's ``sys.path``.  When UDFs imported from ``utils.*`` are
# serialised to workers, cloudpickle tries to re-import them and fails.
#
# The production DAGs solve this by shipping a ``utils.zip`` via the
# ``py_files`` parameter of ``SparkSubmitOperator``.  For tests we use
# the equivalent ``SparkContext.addPyFile()``.
_UTILS_ZIP_PATH = None  # type: str | None


def _build_utils_zip() -> str:
    """Build a temporary ``utils.zip`` and return its path.

    The zip preserves the ``utils/`` package prefix so workers can
    ``from utils.transformations import ...``.
    """
    global _UTILS_ZIP_PATH
    if _UTILS_ZIP_PATH is not None:
        return _UTILS_ZIP_PATH

    utils_dir = os.path.join(_SPARK_JOBS_DIR, "utils")
    if not os.path.isdir(utils_dir):
        raise FileNotFoundError(f"utils directory not found: {utils_dir}")

    fd, zip_path = tempfile.mkstemp(suffix=".zip")
    os.close(fd)
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for root, _, files in os.walk(utils_dir):
            for fname in files:
                if not fname.endswith(".py"):
                    continue
                src = os.path.join(root, fname)
                arc = os.path.relpath(src, _SPARK_JOBS_DIR)
                zf.write(src, arcname=arc)

    _UTILS_ZIP_PATH = zip_path
    return zip_path


def _create_spark_session():
    """Build a fresh local SparkSession for unit tests."""
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("W3C_ETL_Test")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.adaptive.enabled", "false")  # deterministic plans for tests
        .config("spark.ui.enabled", "false")  # no UI overhead
        .getOrCreate()
    )

    # Ship the ``utils/`` package to PySpark workers so that UDFs
    # depending on ``utils.transformations``, ``utils.ua_parser`` etc.
    # can be deserialised without ``ModuleNotFoundError``.
    session.sparkContext.addPyFile(_build_utils_zip())

    return session


def _spark_session_is_alive(session) -> bool:
    """Return True when the SparkSession JVM context is still active."""
    try:
        return session.sparkContext._jsc is not None
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
        SparkSession._instantiatedSession = None
        SparkSession._activeSession = None

    session = _create_spark_session()
    if not _spark_session_is_alive(session):
        try:
            session.stop()
        except Exception:
            pass
        SparkSession._instantiatedSession = None
        SparkSession._activeSession = None
        session = _create_spark_session()

    yield session

    if not _spark_session_is_alive(session):
        SparkSession._instantiatedSession = None
        SparkSession._activeSession = None
