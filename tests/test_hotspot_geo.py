"""Tests for the decomposed geo dimension helpers."""

import os
import sys
from unittest.mock import MagicMock, patch

import pandas as pd

_TEST_DIR = os.path.dirname(os.path.abspath(__file__))
_PLUGINS_DIR = os.path.join(_TEST_DIR, "..", "pipeline", "plugins")
if _PLUGINS_DIR not in sys.path:
    sys.path.insert(0, _PLUGINS_DIR)

from operators import export_dimensions  # noqa: E402
from operators.export_dimensions import (  # noqa: E402
    _build_geo_insert_data,
    _resolve_silver_path,
    _write_geo_with_retry,
)


def test_resolve_silver_path_explicit():
    assert _resolve_silver_path("/tmp/silver") == "/tmp/silver"


def test_resolve_silver_path_env(monkeypatch):
    monkeypatch.setenv("W3C_SILVER_PATH", "/env/silver")
    assert _resolve_silver_path(None) == "/env/silver"


def test_build_geo_insert_data_applies_defaults():
    geo = pd.DataFrame([
        {
            "client_ip": " 10.0.0.1 ",
            "country": None,
            "region": "",
            "city": "Example",
            "postcode": None,
            "latitude": float("nan"),
            "longitude": 1.5,
            "isp": None,
        }
    ])
    (row,) = _build_geo_insert_data(geo)
    assert row[0] == "10.0.0.1"
    assert row[1:] == ("Unknown", "Unknown", "Example", "-", None, 1.5, "-")


def test_write_geo_with_retry_commits_once():
    insert_data = [("10.0.0.1", "Unknown", "Unknown", "Unknown", "-", None, None, "-")]
    conn = MagicMock()
    ctx = MagicMock()
    ctx.__enter__.return_value = conn
    ctx.__exit__.return_value = False
    with (
        patch.object(export_dimensions, "get_conn", return_value=ctx),
        patch.object(export_dimensions, "_write_geo_versions") as mock_write,
    ):
        _write_geo_with_retry(insert_data)
    mock_write.assert_called_once_with(conn, insert_data)
    conn.commit.assert_called_once()


def test_write_geo_with_retry_jitters_backoff():
    import psycopg2

    insert_data = [("10.0.0.1", "Unknown", "Unknown", "Unknown", "-", None, None, "-")]
    conn = MagicMock()
    ctx = MagicMock()
    ctx.__enter__.return_value = conn
    ctx.__exit__.return_value = False
    sleeps = []

    def flaky(conn, data):
        if not sleeps:
            raise psycopg2.OperationalError("transient")

    with (
        patch.object(export_dimensions, "get_conn", return_value=ctx),
        patch.object(export_dimensions, "_write_geo_versions", side_effect=flaky),
        patch.object(export_dimensions, "random") as mock_random,
        patch.object(export_dimensions.time, "sleep", side_effect=sleeps.append),
    ):
        mock_random.uniform.return_value = 1.0
        _write_geo_with_retry(insert_data)

    # First attempt sleeps for the base 2s delay plus 1.0s of jitter.
    assert sleeps == [3.0]
