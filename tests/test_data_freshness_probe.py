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
