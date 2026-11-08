"""Unit tests for the data-freshness probe's row-format contract.

The probe imports pymssql and prometheus_client at module load; neither is
installed in the test environment, so both are stubbed here to exercise the
module DB-free.

The regression being pinned: every query in the probe indexes ``row[0]``,
so Azure SQL connections must yield positional rows. pymssql's
``as_dict=True`` returns dict rows instead — a KeyError on Azure SQL only,
silently swallowed by the probe's exception handlers (PostgreSQL always
returns positional rows, so the bug only surfaced in production).
"""

from __future__ import annotations

import importlib
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

import pytest

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_SCRIPTS_DIR = _PROJECT_ROOT / "pipeline" / "scripts"


@pytest.fixture()
def probe(monkeypatch):
    """Import pipeline/scripts/data_freshness_probe.py with stubbed drivers."""
    monkeypatch.setitem(sys.modules, "pymssql", mock.MagicMock())
    monkeypatch.setitem(sys.modules, "prometheus_client", mock.MagicMock())
    monkeypatch.syspath_prepend(str(_SCRIPTS_DIR))
    module = importlib.import_module("data_freshness_probe")
    yield module
    sys.modules.pop("data_freshness_probe", None)


def test_azure_connect_forces_positional_rows(probe, monkeypatch):
    """pymssql must be told as_dict=False — dict rows break every row[0]."""
    assert "as_dict" not in probe.AZURE_DSN, (
        "as_dict must stay out of AZURE_DSN: dict values gate HAS_AZURE_SQL, "
        "so a False value there would silently disable the Azure backend"
    )
    monkeypatch.setattr(probe, "HAS_AZURE_SQL", True)
    with mock.patch.object(probe.pymssql, "connect") as connect:
        probe._get_connection()
    connect.assert_called_once_with(**probe.AZURE_DSN, as_dict=False)


def test_check_freshness_reads_positional_rows(probe, monkeypatch):
    """The shared positional path must work for tuple rows from either backend."""
    loaded_at = datetime.now(timezone.utc) - timedelta(hours=1)
    cursor = mock.MagicMock()
    cursor.fetchone.return_value = (loaded_at,)
    conn = mock.MagicMock()
    conn.cursor.return_value = cursor
    monkeypatch.setattr(probe, "HAS_AZURE_SQL", True)
    with mock.patch.object(probe.pymssql, "connect", return_value=conn):
        result = probe.check_freshness()
    assert probe.ACTIVE_BACKEND == "azure_sql"
    assert result is not None
    assert 3500 < result < 3700


def _conn_with_single_row(value):
    cursor = mock.MagicMock()
    cursor.fetchone.return_value = (value,)
    conn = mock.MagicMock()
    conn.cursor.return_value = cursor
    return conn, cursor


def test_check_freshness_falls_back_to_max_log_date(probe, monkeypatch):
    """No tracking row yet -> fall back to MAX(log_date), a date (not datetime)."""
    today = datetime.now(timezone.utc).date()
    cursor = mock.MagicMock()
    cursor.fetchone.side_effect = [(None,), (today,)]
    conn = mock.MagicMock()
    conn.cursor.return_value = cursor
    monkeypatch.setattr(probe, "HAS_AZURE_SQL", True)
    with mock.patch.object(probe.pymssql, "connect", return_value=conn):
        result = probe.check_freshness()
    assert result is not None
    assert result >= 0
    assert cursor.execute.call_count == 2
    assert "MAX(log_date)" in cursor.execute.call_args[0][0]


def test_check_dbt_pass_rate_is_zero_without_a_database(probe, monkeypatch):
    monkeypatch.setattr(probe, "_get_connection", lambda: (None, None))
    assert probe.check_dbt_pass_rate() == 0.0


def test_check_dbt_pass_rate_is_one_when_table_missing(probe, monkeypatch):
    conn, cursor = _conn_with_single_row(0)
    monkeypatch.setattr(probe, "HAS_AZURE_SQL", True)
    with mock.patch.object(probe.pymssql, "connect", return_value=conn):
        assert probe.check_dbt_pass_rate() == 1.0
    assert cursor.execute.call_count == 1  # never queries a missing table


def test_check_dbt_pass_rate_computes_ratio(probe, monkeypatch):
    cursor = mock.MagicMock()
    cursor.fetchone.side_effect = [(1,), (0.75,)]
    conn = mock.MagicMock()
    conn.cursor.return_value = cursor
    monkeypatch.setattr(probe, "HAS_AZURE_SQL", True)
    with mock.patch.object(probe.pymssql, "connect", return_value=conn):
        assert probe.check_dbt_pass_rate() == 0.75


def test_check_warehouse_row_count_reads_first_column(probe, monkeypatch):
    conn, cursor = _conn_with_single_row(42)
    monkeypatch.setattr(probe, "HAS_AZURE_SQL", True)
    with mock.patch.object(probe.pymssql, "connect", return_value=conn):
        assert probe.check_warehouse_row_count() == 42
    assert "COUNT(*)" in cursor.execute.call_args[0][0]


def test_check_warehouse_row_count_is_zero_on_query_failure(probe, monkeypatch):
    conn = mock.MagicMock()
    conn.cursor.side_effect = RuntimeError("boom")
    monkeypatch.setattr(probe, "HAS_AZURE_SQL", True)
    with mock.patch.object(probe.pymssql, "connect", return_value=conn):
        assert probe.check_warehouse_row_count() == 0


def test_query_databricks_returns_none_when_statement_not_succeeded(probe):
    payload = mock.MagicMock()
    payload.read.return_value = b'{"status": {"state": "FAILED"}}'
    payload.__enter__ = mock.MagicMock(return_value=payload)
    payload.__exit__ = mock.MagicMock(return_value=False)
    with mock.patch.object(probe.urllib.request, "urlopen", return_value=payload):
        assert probe.query_databricks("SELECT 1") is None


def test_query_databricks_returns_data_array(probe):
    payload = mock.MagicMock()
    payload.read.return_value = b'{"status": {"state": "SUCCEEDED"}, "result": {"data_array": [["7"]]}}'
    payload.__enter__ = mock.MagicMock(return_value=payload)
    payload.__exit__ = mock.MagicMock(return_value=False)
    with mock.patch.object(probe.urllib.request, "urlopen", return_value=payload):
        assert probe.query_databricks("SELECT COUNT(*)") == [["7"]]


def test_check_databricks_row_count_parses_and_defaults(probe, monkeypatch):
    monkeypatch.setattr(probe, "query_databricks", lambda sql: [["7"]])
    assert probe.check_databricks_row_count("main.bronze.logs") == 7
    monkeypatch.setattr(probe, "query_databricks", lambda sql: None)
    assert probe.check_databricks_row_count("main.bronze.logs") == 0


def _import_with_env(monkeypatch):
    """Import the probe module fresh, honouring env vars set by the caller."""
    monkeypatch.setitem(sys.modules, "pymssql", mock.MagicMock())
    monkeypatch.setitem(sys.modules, "prometheus_client", mock.MagicMock())
    monkeypatch.syspath_prepend(str(_SCRIPTS_DIR))
    try:
        return importlib.import_module("data_freshness_probe")
    finally:
        sys.modules.pop("data_freshness_probe", None)


def test_env_int_reads_value_and_defaults_when_unset(probe, monkeypatch):
    monkeypatch.setenv("PROBE_UNDER_TEST", "60")
    assert probe._env_int("PROBE_UNDER_TEST", 900) == 60
    monkeypatch.delenv("PROBE_UNDER_TEST")
    assert probe._env_int("PROBE_UNDER_TEST", 900) == 900
    monkeypatch.setenv("PROBE_UNDER_TEST", "")
    assert probe._env_int("PROBE_UNDER_TEST", 900) == 900


def test_env_int_falls_back_on_malformed_value(probe, monkeypatch, caplog):
    monkeypatch.setenv("PROBE_UNDER_TEST", "fifteen minutes")
    assert probe._env_int("PROBE_UNDER_TEST", 900) == 900
    assert "Invalid PROBE_UNDER_TEST" in caplog.text


def test_port_and_interval_are_env_configurable(monkeypatch):
    monkeypatch.setenv("PROBE_PORT", "9111")
    monkeypatch.setenv("PROBE_INTERVAL_SECONDS", "60")
    module = _import_with_env(monkeypatch)
    assert module.PROBE_PORT == 9111
    assert module.PROBE_INTERVAL_SECONDS == 60


def test_malformed_interval_does_not_break_import(monkeypatch):
    monkeypatch.setenv("PROBE_INTERVAL_SECONDS", "fifteen minutes")
    module = _import_with_env(monkeypatch)
    assert module.PROBE_INTERVAL_SECONDS == 900


def test_warehouse_row_count_logs_query_failure(probe, monkeypatch, caplog):
    conn = mock.MagicMock()
    conn.cursor.side_effect = RuntimeError("boom")
    monkeypatch.setattr(probe, "HAS_AZURE_SQL", True)
    with mock.patch.object(probe.pymssql, "connect", return_value=conn):
        assert probe.check_warehouse_row_count() == 0
    assert "row-count query failed" in caplog.text


def test_freshness_failure_is_logged_not_swallowed(probe, monkeypatch, caplog):
    conn = mock.MagicMock()
    conn.cursor.side_effect = RuntimeError("boom")
    monkeypatch.setattr(probe, "HAS_AZURE_SQL", True)
    with mock.patch.object(probe.pymssql, "connect", return_value=conn):
        assert probe.check_freshness() is None
    assert "freshness query failed" in caplog.text


# ── Databricks cursor retry ───────────────────────────────────────────
# A serverless warehouse that scaled to zero answers RUNNING, and a cold
# endpoint can drop the connection. Neither is a data problem, so the probe
# retries before reporting 0 rows.


def _response(body: bytes):
    """Build a urlopen-style context manager returning ``body``."""
    payload = mock.MagicMock()
    payload.read.return_value = body
    payload.__enter__ = mock.MagicMock(return_value=payload)
    payload.__exit__ = mock.MagicMock(return_value=False)
    return payload


_SUCCEEDED = b'{"status": {"state": "SUCCEEDED"}, "result": {"data_array": [["7"]]}}'
_RUNNING = b'{"status": {"state": "RUNNING"}}'
_FAILED = b'{"status": {"state": "FAILED"}}'


def test_query_databricks_retries_connection_error(probe, monkeypatch):
    monkeypatch.setattr(probe.time, "sleep", mock.MagicMock())
    with mock.patch.object(
        probe.urllib.request,
        "urlopen",
        side_effect=[probe.urllib.error.URLError("warehouse cold"), _response(_SUCCEEDED)],
    ) as urlopen:
        assert probe.query_databricks("SELECT 1") == [["7"]]
    assert urlopen.call_count == 2


def test_query_databricks_retries_running_statement(probe, monkeypatch):
    monkeypatch.setattr(probe.time, "sleep", mock.MagicMock())
    with mock.patch.object(
        probe.urllib.request,
        "urlopen",
        side_effect=[_response(_RUNNING), _response(_SUCCEEDED)],
    ) as urlopen:
        assert probe.query_databricks("SELECT 1") == [["7"]]
    assert urlopen.call_count == 2


def test_query_databricks_gives_up_after_all_attempts(probe, monkeypatch):
    monkeypatch.setattr(probe.time, "sleep", mock.MagicMock())
    with mock.patch.object(
        probe.urllib.request,
        "urlopen",
        side_effect=probe.urllib.error.URLError("down"),
    ) as urlopen:
        assert probe.query_databricks("SELECT 1") is None
    assert urlopen.call_count == probe.DATABRICKS_RETRY_ATTEMPTS


def test_query_databricks_does_not_retry_terminal_state(probe, monkeypatch):
    sleep = mock.MagicMock()
    monkeypatch.setattr(probe.time, "sleep", sleep)
    with mock.patch.object(probe.urllib.request, "urlopen", return_value=_response(_FAILED)) as urlopen:
        assert probe.query_databricks("SELECT 1") is None
    assert urlopen.call_count == 1
    sleep.assert_not_called()
