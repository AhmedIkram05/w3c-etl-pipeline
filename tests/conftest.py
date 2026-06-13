"""
PySpark test fixtures for the W3C ETL medallion pipeline.

Provides a shared SparkSession for integration tests that need to
exercise PySpark UDFs and DataFrame operations.

Adds necessary project paths to sys.path (without adding the project
root itself) so test modules can import using fully-qualified names::

    from utils.transformations import page_category
    from plugins.operators.export_dimensions import _parse_user_agent
    from dags.w3c.spark_ingestion import _export_dimensions

Note: ``dag_integrity`` tests are excluded from CI via marker filter
``not dag_integrity`` because the project's ``airflow/`` directory
shadows the installed ``apache-airflow`` package (PEP 420 namespace
package without ``__init__.py``).
"""

import os
import sys
import tempfile
import zipfile

import pytest

# ── Airflow home (prevents default ~/airflow database path) ────────
os.environ.setdefault("AIRFLOW_HOME", tempfile.mkdtemp(prefix="af_home_"))

# ── Remove project root from sys.path ────────────────────────────────
# Pytest auto-adds the project root to sys.path before conftest.py is
# even loaded.  The project's ``airflow/`` directory has no
# ``__init__.py``, so Python 3.3+ treats it as a PEP 420 namespace
# package.  If the project root stays on sys.path, ``import airflow``
# resolves to this namespace package instead of the installed
# ``apache-airflow`` package, causing:
#
#     ModuleNotFoundError: No module named 'airflow.models'; 'airflow'
#     is not a package
#
# We strip the project root here and NEVER add it back.

_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

# Remove ALL sys.path entries that resolve to the project root.
# Pytest adds the rootdir explicitly; additionally, the empty-string
# entry '' resolves to the CWD (which is also the project root in CI).
# Removing just the first hit via break() leaves '' on the path, which
# still allows Python to discover the local airflow/ namespace.
sys.path = [p for p in sys.path if os.path.abspath(p) != _PROJECT_ROOT]

# ── Add only the specific subdirectories needed for test imports ─────
# Adding the project root would re-introduce the airflow namespace
# shadowing, so we never add it back.
#
# Path resolution note: In Docker the project's ``airflow/`` subdirectories
# (dags/, spark/, dbt/, plugins/) are volume-mounted directly under the
# project root (/opt/airflow), so there is no ``airflow/`` container dir.
# On bare metal the ``airflow/`` directory exists.  We check both layouts.


def _resolve_airflow_path(*subdirs: str) -> str | None:
    """Return the first existing path from two possible layouts.

    Tries ``<project_root>/airflow/<subdirs>`` (bare metal) first,
    then ``<project_root>/<subdirs>`` (Docker volume mount).
    Returns ``None`` if neither exists.
    """
    for layout in (
        os.path.join(_PROJECT_ROOT, "airflow", *subdirs),
        os.path.join(_PROJECT_ROOT, *subdirs),
    ):
        if os.path.isdir(layout):
            return layout
    return None


# Airflow root directory (so "plugins.operators.export_dimensions" resolves).
# IMPORTANT: In Docker the project root IS the airflow root (since ``airflow/``
# subdirectories are volume-mounted directly under ``/opt/airflow/``).  Adding
# the project root back to sys.path would re-introduce the PEP 420 namespace
# shadowing, so we skip re-adding ``_AIRFLOW_DIR`` when it equals the project
# root.
_AIRFLOW_DIR = _resolve_airflow_path()
if _AIRFLOW_DIR is not None and _AIRFLOW_DIR != _PROJECT_ROOT and _AIRFLOW_DIR not in sys.path:
    sys.path.insert(0, _AIRFLOW_DIR)

# Spark jobs directory (for utils/ module imports)
_SPARK_JOBS_DIR = _resolve_airflow_path("spark", "jobs")
if _SPARK_JOBS_DIR is not None and _SPARK_JOBS_DIR not in sys.path:
    sys.path.insert(0, _SPARK_JOBS_DIR)

# Airflow dags directory (so "dags.w3c.spark_ingestion" resolves)
_DAGS_DIR = _resolve_airflow_path("dags")
if _DAGS_DIR is not None and _DAGS_DIR not in sys.path:
    sys.path.insert(0, _DAGS_DIR)

# Airflow plugins directory (so "operators.export_csv_azure" resolves)
_PLUGINS_DIR = _resolve_airflow_path("plugins")
if _PLUGINS_DIR is not None and _PLUGINS_DIR not in sys.path:
    sys.path.insert(0, _PLUGINS_DIR)


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

    if _SPARK_JOBS_DIR is None:
        raise FileNotFoundError(
            f"spark/jobs directory not found. Tried:\n"
            f"  {os.path.join(_PROJECT_ROOT, 'airflow', 'spark', 'jobs')}\n"
            f"  {os.path.join(_PROJECT_ROOT, 'spark', 'jobs')}"
        )
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
