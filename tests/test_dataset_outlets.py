"""
Behavioral tests for programmatic Dataset outlet emission (#12).

The ingestion DAGs emit their downstream Dataset outlets *programmatically*
inside the task callables (Airflow ``outlet_events``) instead of declaring
them statically on the operators. A declared outlet fires whenever the task
succeeds — even when the callable took a graceful skip (e.g. missing Azure
SQL credentials) — which would wake the dataset-triggered dbt DAGs against
stale data. These tests pin the emit-on-real-success contract:

* Azure DAG (``spark_ingestion_azure``): ``_export_dimensions`` emits the
  dim datasets, ``_create_indexes`` emits the warehouse-loaded dataset —
  each only after its work actually succeeded.
* Local DAG (``spark_ingestion``): the ``export_dimensions`` wrapper emits
  ``WAREHOUSE_LOADED`` only after the dimension build returns.

All Azure SQL / PostgreSQL interactions are mocked (hermetic, no DB).
"""

from __future__ import annotations

import os
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("airflow")

# ── Module under test (imports the DAG files, as DagBag does) ─────────────
from dags.w3c import spark_ingestion as local_dag  # noqa: E402
from dags.w3c.spark_ingestion_azure import (  # noqa: E402
    AZURE_WAREHOUSE_LOADED,
    DIM_GEOLOCATION,
    DIM_USERAGENT,
    _create_indexes,
    _export_dimensions,
)

# ── Fake Airflow outlet_events context ────────────────────────────────────


class _OutletEventRecorder:
    """Stand-in for Airflow's ``OutletEventAccessor`` — counts emit() calls."""

    def __init__(self):
        self.emitted = 0

    def emit(self, *args, **kwargs):
        self.emitted += 1


class _FakeOutletEvents(dict):
    """dict[Dataset, recorder] that lazily creates recorders, like Airflow."""

    def __getitem__(self, key):
        return self.setdefault(key, _OutletEventRecorder())


def _events() -> _FakeOutletEvents:
    return _FakeOutletEvents()


# ── pyodbc mock (same pattern as test_export_dimensions_azure.py) ─────────


class _PyodbcError(Exception):
    """Stand-in for ``pyodbc.Error``."""


@contextmanager
def _mock_pyodbc(ua_rows=None, fail_on_execute=False):
    mock_pyodbc = MagicMock(name="pyodbc")
    mock_conn = MagicMock(name="conn")
    mock_cursor = MagicMock(name="cursor")

    mock_pyodbc.connect.return_value.__enter__.return_value = mock_conn
    mock_pyodbc.Error = _PyodbcError
    mock_conn.cursor.return_value = mock_cursor

    if fail_on_execute:
        mock_cursor.execute.side_effect = _PyodbcError("simulated DB error")
    if ua_rows is not None:
        mock_cursor.fetchall.return_value = ua_rows

    with patch.dict("sys.modules", {"pyodbc": mock_pyodbc}, clear=False):
        yield mock_pyodbc


_AZURE_CREDS = {
    "AZURE_SQL_SERVER": "s",
    "AZURE_SQL_USER": "u",
    "AZURE_SQL_PASS": "p",
}


# ═══════════════════════════════════════════════════════════════════════════
# Azure DAG — _export_dimensions
# ═══════════════════════════════════════════════════════════════════════════


class TestAzureDimensionsOutlet:
    """``_export_dimensions`` emits DIM_GEOLOCATION/DIM_USERAGENT on real success."""

    def test_emits_both_dims_on_success(self):
        events = _events()
        with patch.dict(os.environ, _AZURE_CREDS, clear=True):
            with _mock_pyodbc(ua_rows=[]):
                _export_dimensions(outlet_events=events)

        assert events[DIM_GEOLOCATION].emitted == 1
        assert events[DIM_USERAGENT].emitted == 1

    def test_no_emit_on_missing_credentials(self):
        """Graceful skip (no Azure SQL creds) must NOT emit the dim outlets."""
        events = _events()
        with patch.dict(os.environ, {}, clear=True):
            _export_dimensions(outlet_events=events)

        assert len(events) == 0, "Outlets were emitted despite a graceful skip"

    def test_no_emit_on_pyodbc_error(self):
        events = _events()
        with patch.dict(os.environ, _AZURE_CREDS, clear=True):
            with _mock_pyodbc(fail_on_execute=True):
                with pytest.raises(_PyodbcError):
                    _export_dimensions(outlet_events=events)

        assert len(events) == 0, "Outlets were emitted despite a failed dimension build"

    def test_no_context_is_a_noop(self):
        """Direct invocation without an Airflow task context emits nothing."""
        with patch.dict(os.environ, {}, clear=True):
            assert _export_dimensions() is None


# ═══════════════════════════════════════════════════════════════════════════
# Azure DAG — _create_indexes
# ═══════════════════════════════════════════════════════════════════════════


class TestAzureIndexesOutlet:
    """``_create_indexes`` emits AZURE_WAREHOUSE_LOADED on real success."""

    def test_emits_warehouse_loaded_on_success(self):
        events = _events()
        with patch.dict(os.environ, _AZURE_CREDS, clear=True):
            with _mock_pyodbc():
                _create_indexes(outlet_events=events)

        assert events[AZURE_WAREHOUSE_LOADED].emitted == 1

    def test_no_emit_on_missing_credentials(self):
        events = _events()
        with patch.dict(os.environ, {}, clear=True):
            _create_indexes(outlet_events=events)

        assert len(events) == 0, "Warehouse-loaded outlet emitted despite a graceful skip"


# ═══════════════════════════════════════════════════════════════════════════
# Local DAG — export_dimensions wrapper
# ═══════════════════════════════════════════════════════════════════════════


class TestLocalDimensionsOutlet:
    """``_export_dimensions_with_outlet`` emits WAREHOUSE_LOADED after success."""

    def test_emits_after_successful_build(self, monkeypatch):
        events = _events()
        calls = []
        monkeypatch.setattr(local_dag, "_export_dimensions", lambda: calls.append(1))

        local_dag._export_dimensions_with_outlet(outlet_events=events)

        assert calls == [1]
        assert events[local_dag.WAREHOUSE_LOADED].emitted == 1

    def test_no_emit_when_build_raises(self, monkeypatch):
        events = _events()

        def _boom():
            raise RuntimeError("dimension build failed")

        monkeypatch.setattr(local_dag, "_export_dimensions", _boom)

        with pytest.raises(RuntimeError):
            local_dag._export_dimensions_with_outlet(outlet_events=events)

        assert len(events) == 0, "Warehouse-loaded outlet emitted despite a failed build"

    def test_no_context_is_a_noop(self, monkeypatch):
        monkeypatch.setattr(local_dag, "_export_dimensions", lambda: None)
        assert local_dag._export_dimensions_with_outlet() is None
