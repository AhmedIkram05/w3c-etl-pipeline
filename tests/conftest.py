"""
PySpark test fixtures for the W3C ETL medallion pipeline.

Provides a shared SparkSession for integration tests that need to
exercise PySpark UDFs and DataFrame operations.

Adds necessary project paths to sys.path (without adding the project
root itself) so test modules can import using fully-qualified names::

    from utils.transformations import page_category
    from plugins.operators.export_dimensions import _parse_user_agent
    from dags.w3c.spark_ingestion import _export_dimensions
"""

import os
import sys
import tempfile
import zipfile

import pytest

# ── Airflow home (prevents default ~/airflow database path) ────────
os.environ.setdefault("AIRFLOW_HOME", tempfile.mkdtemp(prefix="af_home_"))

# ── PySpark workers use the test interpreter ─────────────────────────
# Local-mode workers default to ``python3`` from PATH, which may lack the
# test env's third-party UDF deps (user_agents, geoip2, pandas). Pinning
# workers to the driver interpreter keeps worker imports working in any
# layout. Must precede the first JVM launch below.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)

# ── Add only the specific subdirectories needed for test imports ─────
# Adding the project root would re-introduce a directory named like an
# installed third-party package, so we never add it back.
#
# Path resolution note: In Docker the project's pipeline subdirectories
# (dags/, spark/, dbt/, plugins/) are volume-mounted directly under the
# project root (/opt/airflow), so there is no ``pipeline/`` container dir.
# On bare metal the ``pipeline/`` directory exists.  We check both layouts.


def _resolve_pipeline_path(*subdirs: str) -> str | None:
    """Return the first existing path from two possible layouts.

    Tries ``<project_root>/pipeline/<subdirs>`` (bare metal) first,
    then ``<project_root>/<subdirs>`` (Docker volume mount).
    Returns ``None`` if neither exists.
    """
    for layout in (
        os.path.join(_PROJECT_ROOT, "pipeline", *subdirs),
        os.path.join(_PROJECT_ROOT, *subdirs),
    ):
        if os.path.isdir(layout):
            return layout
    return None


# Pipeline root directory (so "plugins.operators.export_dimensions" resolves).
# IMPORTANT: In Docker the project root IS the pipeline root (since the
# pipeline subdirectories are volume-mounted directly under ``/opt/airflow/``).
# Adding the project root back to sys.path would re-introduce a directory
# whose name collides with the installed apache-airflow package, so we skip
# re-adding ``_PIPELINE_DIR`` when it equals the project root.
_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
_PIPELINE_DIR = _resolve_pipeline_path()
if _PIPELINE_DIR is not None and _PIPELINE_DIR != _PROJECT_ROOT and _PIPELINE_DIR not in sys.path:
    sys.path.insert(0, _PIPELINE_DIR)

# Spark jobs directory (for utils/ module imports)
_SPARK_JOBS_DIR = _resolve_pipeline_path("spark", "jobs")
if _SPARK_JOBS_DIR is not None and _SPARK_JOBS_DIR not in sys.path:
    sys.path.insert(0, _SPARK_JOBS_DIR)

# Pipeline dags directory (so "dags.w3c.spark_ingestion" resolves)
_DAGS_DIR = _resolve_pipeline_path("dags")
if _DAGS_DIR is not None and _DAGS_DIR not in sys.path:
    sys.path.insert(0, _DAGS_DIR)

# Pipeline plugins directory (so "operators.export_csv_azure" resolves)
_PLUGINS_DIR = _resolve_pipeline_path("plugins")
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
            f"  {os.path.join(_PROJECT_ROOT, 'pipeline', 'spark', 'jobs')}\n"
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
        .config("spark.jars.packages", "io.delta:delta-spark_2.13:4.0.1")
        .config("spark.sql.adaptive.enabled", "false")  # deterministic plans for tests
        .config("spark.ui.enabled", "false")  # no UI overhead
        .getOrCreate()
    )

    # Ship the ``utils/`` package to PySpark workers so that UDFs
    # depending on ``utils.transformations``, ``utils.ua_parser`` etc.
    # can be deserialised without ``ModuleNotFoundError``.
    session.sparkContext.addPyFile(_build_utils_zip())

    return session


# ── Own the first JVM ────────────────────────────────────────────────
# ``spark.jars.packages`` is resolved only once per process: when the very
# first SparkContext launches the JVM.  Some test modules create sessions at
# module import (e.g. the probe in test_jdbc_export_azure) and ``stop()``
# does NOT free that JVM, so a delta-less first builder poisons every later
# getOrCreate() in the run (`ClassNotFoundException: ...DeltaCatalog`).
# Conftest is imported before every test module, so building our delta
# configured session here — and never stopping it — guarantees the first
# JVM is always the right one, no matter which fixture fires first.
try:
    _HOLD_FIRST_SESSION = _create_spark_session()
except ImportError:
    # pyspark isn't installed (e.g. the dbt-compile job only runs pytest) —
    # nothing to hold, and no spark fixture will be requested there.
    _HOLD_FIRST_SESSION = None


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
