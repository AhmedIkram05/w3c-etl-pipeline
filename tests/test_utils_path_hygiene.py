"""Regression tests for the driver-side ``utils.zip`` binding bug (phase-2).

``SparkContext.addPyFile(zip)`` inserts the SparkFiles copy of ``utils.zip``
into the *driver's* ``sys.path`` (workers get it separately via
``_python_includes``).  Before the fix, the first ``import utils`` then bound
the package to that ephemeral copy, and a later ``spark.stop()`` (the jdbc
export E2E tests) deleted the directory — leaving ``utils.__path__``
dangling, so any submodule not yet imported raised ``ModuleNotFoundError``
(observed as ``No module named 'utils.transformations'`` in
``test_transformations`` / ``test_silver_backfill`` while ``utils.geoip``,
imported before the stop, still resolved from ``sys.modules``).

conftest now strips the driver entry after every ``addPyFile``; the driver
resolves ``utils`` from the on-disk ``spark/jobs/utils`` package.
"""

import importlib

import pytest

pytest.importorskip("pyspark")


def test_driver_utils_resolves_from_filesystem(spark):
    """After session creation, ``utils`` must not be bound to a SparkFiles zip."""
    utils_mod = importlib.import_module("utils")
    file_ = getattr(utils_mod, "__file__", "") or ""
    assert "userFiles" not in file_, f"utils bound to ephemeral SparkFiles zip: {file_}"


def test_utils_importable_after_session_stop(spark):
    """Stopping the session must not strand not-yet-imported ``utils`` submodules."""
    spark.stop()  # replicates the jdbc-export E2E teardown
    for name in ("utils.transformations", "utils.ua_parser", "utils.geoip", "utils.schemas"):
        importlib.import_module(name)
