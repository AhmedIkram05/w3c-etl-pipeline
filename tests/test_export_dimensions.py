"""
Unit tests for ``export_dimensions.py`` — Airflow-managed dimension tables.

The operator builds two dimension tables from local sources (no external
HTTP / IP-API calls — that path was retired):

* ``dim_geolocation``  — sourced from the **Silver Delta table**
  (``pd.read_parquet`` of ``W3C_SILVER_PATH``), which is pre-enriched by
  the Spark ``silver_enrichment`` job using local MaxMind GeoLite2
  databases.  The operator collapses distinct ``client_ip`` rows and
  upserts them via ``INSERT … ON CONFLICT (ip) DO NOTHING``.
* ``dim_useragent``    — parsed from ``raw_enriched`` via the
  ``user-agents`` library.

All tests use ``unittest.mock`` to stub out ``pd.read_parquet``,
``get_conn``, and ``execute_values`` so the suite is fully hermetic.
Should run with just ``pytest`` once the test requirements are
installed (``pandas``, ``psycopg2``, ``user-agents``).

Usage:
    pytest tests/test_export_dimensions.py -v --tb=short
"""

from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from plugins.operators.export_dimensions import (
    DEFAULT_SILVER_PATH,
    _build_dim_geolocation,
    _build_dim_useragent,
    _coalesce,
    _ensure_default_rows,
    _ensure_dimension_tables,
    _parse_user_agent,
    _read_silver_geo_dim,
    export_dimensions,
    get_conn,
)

# ═══════════════════════════════════════════════════════════════════════════
# Test fixtures and helpers
# ═══════════════════════════════════════════════════════════════════════════


@contextmanager
def _patched_write_conn():
    """Patch ``get_conn`` to return a context-managed mock connection.

    The operator calls ``with get_conn() as write_conn, write_conn.cursor() as write_cur:``
    inside its retry loop, so the mock has to be a context manager at
    two levels: the connection itself and the cursor it returns.
    """
    with patch("plugins.operators.export_dimensions.get_conn") as mock_get_conn:
        mock_write_conn = MagicMock(name="write_conn")
        mock_write_cur = MagicMock(name="write_cur")
        # get_conn() is the outer "with" target.
        mock_get_conn.return_value.__enter__.return_value = mock_write_conn
        # write_conn.cursor() is the inner "with" target.
        mock_write_conn.cursor.return_value.__enter__.return_value = mock_write_cur
        yield mock_get_conn, mock_write_conn, mock_write_cur


def _make_read_conn(existing_ips=()):
    """Build a mock read-side connection used by the dimension writers.

    The operator does ``conn.cursor()`` (not as a context manager) and
    then ``cur.execute("SELECT DISTINCT ip FROM dim_geolocation")``
    followed by ``cur.fetchall()``.
    """
    mock_conn = MagicMock(name="read_conn")
    mock_cur = MagicMock(name="read_cur")
    mock_cur.fetchall.return_value = [(ip,) for ip in existing_ips]
    mock_conn.cursor.return_value = mock_cur
    return mock_conn, mock_cur


def _make_silver_df(ips, **overrides):
    """Build a Silver Delta DataFrame with one row per IP.

    All optional fields default to plausible Google/Mountain-View values
    so tests can focus on the behavior under test.
    """
    n = len(ips)
    data = {
        "client_ip": ips,
        "country": overrides.get("country", ["US"] * n),
        "region": overrides.get("region", ["California"] * n),
        "city": overrides.get("city", ["Mountain View"] * n),
        "postcode": overrides.get("postcode", ["94043"] * n),
        "latitude": overrides.get("latitude", [37.386] * n),
        "longitude": overrides.get("longitude", [-122.084] * n),
        "isp": overrides.get("isp", ["Google"] * n),
    }
    return pd.DataFrame(data)


# ═══════════════════════════════════════════════════════════════════════════
#  1. _parse_user_agent — pure function tests
# ═══════════════════════════════════════════════════════════════════════════


class TestParseUserAgent:
    """Unit tests for the ``_parse_user_agent()`` helper.

    These tests require the ``user-agents`` library to be installed
    (listed in ``tests/requirements-test.txt``).  The empty / dash
    paths are pure logic and work even with the library stubbed.
    """

    @staticmethod
    def _require_user_agents():
        try:
            import user_agents  # noqa: F401
        except ImportError:
            pytest.skip("user_agents library not installed")

    def test_known_browser(self):
        self._require_user_agents()
        ua = (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        )
        result = _parse_user_agent(ua)
        assert result["BrowserName"] == "Chrome"
        assert result["BrowserVersion"] == "120.0.0"
        assert result["AgentType"] == "Browser"
        assert result["DeviceType"] == "Desktop"
        assert result["OS"] != "Unknown"

    def test_mobile_browser(self):
        self._require_user_agents()
        ua = (
            "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS x) "
            "AppleWebKit/605.1.15 (KHTML, like Gecko) "
            "Version/17.0 Mobile/15E148 Safari/604.1"
        )
        result = _parse_user_agent(ua)
        assert result["AgentType"] == "Browser"
        assert result["DeviceType"] == "Mobile"

    def test_tablet_device(self):
        self._require_user_agents()
        ua = (
            "Mozilla/5.0 (iPad; CPU OS 17_0 like Mac OS x) "
            "AppleWebKit/605.1.15 (KHTML, like Gecko) "
            "Version/17.0 Mobile/15E148 Safari/604.1"
        )
        result = _parse_user_agent(ua)
        assert result["DeviceType"] == "Tablet"

    def test_known_crawler(self):
        self._require_user_agents()
        ua = "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"
        result = _parse_user_agent(ua)
        assert result["AgentType"] == "Crawler"
        assert result["DeviceType"] == "Bot"
        assert "Googlebot" in result["BrowserName"]

    def test_empty_ua_returns_unknown(self):
        """Empty string short-circuits without invoking the parser."""
        result = _parse_user_agent("")
        assert result == {
            "AgentType": "Unknown",
            "BrowserName": "Unknown",
            "BrowserVersion": "Unknown",
            "OS": "Unknown",
            "DeviceType": "Unknown",
        }

    def test_dash_ua_returns_unknown(self):
        """The ``-`` sentinel must also short-circuit."""
        result = _parse_user_agent("-")
        assert result["AgentType"] == "Unknown"
        assert result["BrowserName"] == "Unknown"

    def test_unknown_string_literal_returns_unknown(self):
        result = _parse_user_agent("Unknown")
        assert result["AgentType"] == "Unknown"

    def test_url_encoded_ua_is_decoded(self):
        """``urllib.parse.unquote_plus`` must be applied before parsing."""
        self._require_user_agents()
        ua = "Mozilla%2F5.0+%28Windows+NT+10.0%3B+Win64%3B+x64%29+AppleWebKit%2F537.36"
        result = _parse_user_agent(ua)
        assert result["BrowserName"] != "Unknown"
        assert result["AgentType"] == "Browser"

    def test_whitespace_only_ua(self):
        """Whitespace-only UAs short-circuit just like empty strings."""
        result = _parse_user_agent("   ")
        assert result["AgentType"] == "Unknown"


# ═══════════════════════════════════════════════════════════════════════════
#  2. _coalesce — small helper
# ═══════════════════════════════════════════════════════════════════════════


class TestCoalesce:
    """Verify the ``_coalesce`` helper used when building the INSERT payload."""

    def test_none_returns_default(self):
        assert _coalesce(None, "Unknown") == "Unknown"

    def test_empty_string_returns_default(self):
        assert _coalesce("", "Unknown") == "Unknown"

    def test_dash_string_returns_default(self):
        assert _coalesce("-", "Unknown") == "Unknown"

    def test_non_empty_value_returned_as_is(self):
        assert _coalesce("US", "Unknown") == "US"

    def test_non_string_value_returned_as_is(self):
        assert _coalesce(42, "Unknown") == 42


# ═══════════════════════════════════════════════════════════════════════════
#  3. Default -1 surrogate key rows
# ═══════════════════════════════════════════════════════════════════════════


class TestEnsureDefaultRows:
    """Verify the default -1 surrogate-key rows are inserted correctly."""

    def test_dim_geolocation_default_insert_sql(self):
        """First INSERT seeds ``dim_geolocation`` with the -1 row."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        _ensure_default_rows(mock_conn)

        assert mock_cursor.execute.call_count == 2

        geoloc_sql = mock_cursor.execute.call_args_list[0][0][0]
        assert "INSERT INTO dim_geolocation" in geoloc_sql
        assert "VALUES (-1, 'Unknown', 'Unknown', 'Unknown', 'Unknown', '-', NULL, NULL, '-')" in geoloc_sql
        assert "ON CONFLICT DO NOTHING" in geoloc_sql

    def test_dim_useragent_default_insert_sql(self):
        """Second INSERT seeds ``dim_useragent`` with the -1 row."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        _ensure_default_rows(mock_conn)

        ua_sql = mock_cursor.execute.call_args_list[1][0][0]
        assert "INSERT INTO dim_useragent" in ua_sql
        assert "VALUES (-1, 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown')" in ua_sql
        assert "ON CONFLICT DO NOTHING" in ua_sql

    def test_ensure_default_rows_commits(self):
        """The default-row block commits exactly once."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        _ensure_default_rows(mock_conn)

        mock_conn.commit.assert_called_once()


# ═══════════════════════════════════════════════════════════════════════════
#  4. _read_silver_geo_dim — Silver Delta reader
# ═══════════════════════════════════════════════════════════════════════════


class TestReadSilverDelta:
    """Verify the Silver Delta reader collapses distinct IPs correctly.

    These tests patch ``pd.read_parquet`` at the operator's import site
    so the reader is exercised end-to-end against synthetic DataFrames.
    """

    def test_empty_delta_returns_empty_dataframe(self):
        """An empty parquet file is returned as an empty DataFrame."""
        with patch(
            "plugins.operators.export_dimensions.pd.read_parquet",
            return_value=pd.DataFrame(),
        ) as mock_read:
            result = _read_silver_geo_dim("/some/path")

        assert result.empty
        mock_read.assert_called_once_with("/some/path")

    def test_distinct_ips_are_collapsed(self):
        """Duplicate ``client_ip`` rows collapse to a single row each."""
        df = _make_silver_df(["1.1.1.1", "1.1.1.1", "2.2.2.2", "2.2.2.2", "2.2.2.2"])
        with patch(
            "plugins.operators.export_dimensions.pd.read_parquet",
            return_value=df,
        ):
            result = _read_silver_geo_dim("/some/path")

        assert len(result) == 2
        assert set(result["client_ip"]) == {"1.1.1.1", "2.2.2.2"}

    def test_null_client_ip_is_dropped(self):
        """``dropna(subset=['client_ip'])`` removes rows with no IP."""
        df = _make_silver_df(["1.1.1.1", "2.2.2.2"])
        # Splice a null-IP row into the middle of the frame.
        df = pd.concat([df.iloc[:1], pd.DataFrame([{"client_ip": None}]), df.iloc[1:]], ignore_index=True)
        # The middle row has only client_ip; pad the other columns.
        for col in ("country", "region", "city", "postcode", "latitude", "longitude", "isp"):
            df[col] = df[col].fillna("US" if col == "country" else 0)

        with patch(
            "plugins.operators.export_dimensions.pd.read_parquet",
            return_value=df,
        ):
            result = _read_silver_geo_dim("/some/path")

        assert set(result["client_ip"]) == {"1.1.1.1", "2.2.2.2"}
        assert len(result) == 2

    def test_dash_client_ip_is_dropped(self):
        """The ``-`` sentinel is treated as no IP at all."""
        df = _make_silver_df(["1.1.1.1", "-", "2.2.2.2"])

        with patch(
            "plugins.operators.export_dimensions.pd.read_parquet",
            return_value=df,
        ):
            result = _read_silver_geo_dim("/some/path")

        assert set(result["client_ip"]) == {"1.1.1.1", "2.2.2.2"}

    def test_missing_postcode_column_is_handled(self):
        """Schemas that pre-date the postcode column still work — the
        function adds a ``postcode`` column of ``None`` so downstream
        merge / insert logic can use ``-`` as a default sentinel.
        """
        df = pd.DataFrame({
            "client_ip": ["1.1.1.1"],
            "country": ["US"],
            "region": ["California"],
            "city": ["Mountain View"],
            # no postcode column on purpose
            "latitude": [37.386],
            "longitude": [-122.084],
            "isp": ["Google"],
        })

        with patch(
            "plugins.operators.export_dimensions.pd.read_parquet",
            return_value=df,
        ):
            result = _read_silver_geo_dim("/some/path")

        assert "postcode" in result.columns
        assert result["postcode"].isna().all()


# ═══════════════════════════════════════════════════════════════════════════
#  5. _build_dim_geolocation — Silver → Postgres writer
# ═══════════════════════════════════════════════════════════════════════════


class TestBuildDimGeolocation:
    """Verify the ``dim_geolocation`` writer integrates with the Silver Delta."""

    def test_inserts_new_ips_from_silver(self):
        """New IPs flow through to the INSERT statement with all columns."""
        silver_df = _make_silver_df(["1.1.1.1"])
        read_conn, _ = _make_read_conn(existing_ips=[])

        with patch(
            "plugins.operators.export_dimensions._read_silver_geo_dim",
            return_value=silver_df,
        ):
            with _patched_write_conn():
                with patch("plugins.operators.export_dimensions.execute_values") as mock_exec:
                    _build_dim_geolocation(read_conn, "/silver/path")

                    insert_sql = mock_exec.call_args[0][1]
                    assert "INSERT INTO dim_geolocation" in insert_sql
                    assert "ON CONFLICT (ip) DO NOTHING" in insert_sql

                    insert_data = mock_exec.call_args[0][2]
                    assert len(insert_data) == 1
                    row = insert_data[0]
                    assert row[0] == "1.1.1.1"
                    assert row[1] == "US"
                    assert row[2] == "California"
                    assert row[3] == "Mountain View"
                    assert row[4] == "94043"  # postcode
                    assert row[5] == 37.386  # latitude
                    assert row[6] == -122.084  # longitude
                    assert row[7] == "Google"  # isp

    def test_skips_already_present_ips(self):
        """If every Silver IP is already in the dim, no INSERT runs."""
        silver_df = _make_silver_df(["1.1.1.1"])
        read_conn, _ = _make_read_conn(existing_ips=["1.1.1.1"])

        with patch(
            "plugins.operators.export_dimensions._read_silver_geo_dim",
            return_value=silver_df,
        ):
            with _patched_write_conn():
                with patch("plugins.operators.export_dimensions.execute_values") as mock_exec:
                    _build_dim_geolocation(read_conn, "/silver/path")

                    mock_exec.assert_not_called()

    def test_missing_silver_path_returns_silently(self):
        """``FileNotFoundError`` from the Delta read is caught — no DB I/O."""
        read_conn, read_cur = _make_read_conn()

        with patch(
            "plugins.operators.export_dimensions._read_silver_geo_dim",
            side_effect=FileNotFoundError("delta path missing"),
        ):
            with _patched_write_conn():
                with patch("plugins.operators.export_dimensions.execute_values") as mock_exec:
                    # Must NOT raise.
                    _build_dim_geolocation(read_conn, "/nonexistent")

                    read_cur.execute.assert_not_called()
                    mock_exec.assert_not_called()

    def test_empty_silver_dataframe_returns_silently(self):
        """An empty Silver DataFrame short-circuits the function."""
        read_conn, read_cur = _make_read_conn()

        with patch(
            "plugins.operators.export_dimensions._read_silver_geo_dim",
            return_value=pd.DataFrame(),
        ):
            with _patched_write_conn():
                with patch("plugins.operators.export_dimensions.execute_values") as mock_exec:
                    _build_dim_geolocation(read_conn, "/empty/path")

                    read_cur.execute.assert_not_called()
                    mock_exec.assert_not_called()


# ═══════════════════════════════════════════════════════════════════════════
#  6. _build_dim_useragent — edge cases
# ═══════════════════════════════════════════════════════════════════════════


class TestBuildDimUseragent:
    """Verify edge cases in the user-agent dimension builder."""

    def test_no_rows_in_raw_enriched(self):
        """When ``raw_enriched`` has no rows, no write connection is opened."""
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_cur.fetchall.return_value = []
        mock_conn.cursor.return_value = mock_cur

        _build_dim_useragent(mock_conn)

        # Only the read cursor should have been created (no write connection).
        mock_conn.cursor.assert_called_once()

    def test_ua_truncated_to_1000_chars(self):
        """Very long user-agent strings are truncated to 1000 chars."""
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        long_ua = "Mozilla/" + "X" * 2000
        mock_cur.fetchall.return_value = [(long_ua,)]
        mock_conn.cursor.return_value = mock_cur

        with _patched_write_conn():
            with patch("plugins.operators.export_dimensions.execute_values") as mock_exec:
                _build_dim_useragent(mock_conn)

                insert_data = mock_exec.call_args[0][2]
                assert len(insert_data[0][0]) == 1000


# ═══════════════════════════════════════════════════════════════════════════
#  7. INSERT ... ON CONFLICT DO NOTHING — SQL pattern verification
# ═══════════════════════════════════════════════════════════════════════════


class TestInsertOnConflict:
    """Verify the INSERT SQL for both dimension tables uses ON CONFLICT."""

    def test_dim_geolocation_insert_sql(self):
        """``_build_dim_geolocation`` emits INSERT … ON CONFLICT (ip) DO NOTHING."""
        silver_df = _make_silver_df(["1.1.1.1"])
        read_conn, _ = _make_read_conn(existing_ips=[])

        with patch(
            "plugins.operators.export_dimensions._read_silver_geo_dim",
            return_value=silver_df,
        ):
            with _patched_write_conn():
                with patch("plugins.operators.export_dimensions.execute_values") as mock_exec:
                    _build_dim_geolocation(read_conn, "/silver/path")

                    insert_sql = mock_exec.call_args[0][1]
                    assert "INSERT INTO dim_geolocation" in insert_sql
                    assert "ON CONFLICT (ip) DO NOTHING" in insert_sql
                    # Batch page_size 1000 is the documented default.
                    assert mock_exec.call_args[1]["page_size"] == 1000

    def test_dim_useragent_insert_sql(self):
        """``_build_dim_useragent`` emits INSERT … ON CONFLICT (user_agent) DO NOTHING."""
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_cur.fetchall.return_value = [("Mozilla/5.0 Chrome/120",)]
        mock_conn.cursor.return_value = mock_cur

        with _patched_write_conn():
            with patch("plugins.operators.export_dimensions.execute_values") as mock_exec:
                _build_dim_useragent(mock_conn)

                insert_sql = mock_exec.call_args[0][1]
                assert "INSERT INTO dim_useragent" in insert_sql
                assert "ON CONFLICT (user_agent) DO NOTHING" in insert_sql


# ═══════════════════════════════════════════════════════════════════════════
#  8. SQL queries — what statements the operator issues
# ═══════════════════════════════════════════════════════════════════════════


class TestSQLQueries:
    """Verify the SQL queries issued by the dimension writers."""

    def test_dim_geolocation_checks_existing_ips_first(self):
        """Before inserting, the writer queries which IPs already exist."""
        silver_df = _make_silver_df(["1.1.1.1"])
        read_conn, read_cur = _make_read_conn(existing_ips=["1.1.1.1"])

        with patch(
            "plugins.operators.export_dimensions._read_silver_geo_dim",
            return_value=silver_df,
        ):
            with _patched_write_conn():
                with patch("plugins.operators.export_dimensions.execute_values"):
                    _build_dim_geolocation(read_conn, "/silver/path")

        select_calls = [c[0][0] for c in read_cur.execute.call_args_list]
        assert any("SELECT DISTINCT ip FROM dim_geolocation" in sql for sql in select_calls)

    def test_dim_useragent_selects_distinct_user_agent(self):
        """``_build_dim_useragent`` queries distinct, non-null, non-dash UAs."""
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_cur.fetchall.return_value = []
        mock_conn.cursor.return_value = mock_cur

        _build_dim_useragent(mock_conn)

        select_calls = [c[0][0] for c in mock_cur.execute.call_args_list]
        assert any("SELECT DISTINCT user_agent FROM raw_enriched" in sql for sql in select_calls)
        assert any("user_agent IS NOT NULL" in sql for sql in select_calls)
        assert any("user_agent != '-'" in sql for sql in select_calls)

    def test_silver_path_defaults_to_w3c_silver_path_env(self):
        """When ``silver_path`` is None, the operator reads ``W3C_SILVER_PATH``."""
        silver_df = _make_silver_df(["1.1.1.1"])
        read_conn, _ = _make_read_conn(existing_ips=["1.1.1.1"])

        with patch.dict(
            "plugins.operators.export_dimensions.os.environ",
            {"W3C_SILVER_PATH": "/from/env/silver"},
            clear=False,
        ):
            with patch(
                "plugins.operators.export_dimensions._read_silver_geo_dim",
                return_value=silver_df,
            ) as mock_read:
                with _patched_write_conn():
                    with patch("plugins.operators.export_dimensions.execute_values"):
                        # silver_path=None — fall back to W3C_SILVER_PATH.
                        _build_dim_geolocation(read_conn, silver_path=None)

                mock_read.assert_called_once_with("/from/env/silver")

    def test_silver_path_defaults_to_module_constant_when_env_unset(self):
        """When both ``silver_path`` and ``W3C_SILVER_PATH`` are absent, ``DEFAULT_SILVER_PATH`` is used."""
        silver_df = _make_silver_df(["1.1.1.1"])
        read_conn, _ = _make_read_conn(existing_ips=["1.1.1.1"])

        env_without_silver = {k: v for k, v in __import__("os").environ.items() if k != "W3C_SILVER_PATH"}
        with patch.dict(
            "plugins.operators.export_dimensions.os.environ",
            env_without_silver,
            clear=True,
        ):
            with patch(
                "plugins.operators.export_dimensions._read_silver_geo_dim",
                return_value=silver_df,
            ) as mock_read:
                with _patched_write_conn():
                    with patch("plugins.operators.export_dimensions.execute_values"):
                        _build_dim_geolocation(read_conn, silver_path=None)

                mock_read.assert_called_once_with(DEFAULT_SILVER_PATH)


# ═══════════════════════════════════════════════════════════════════════════
#  9. _ensure_dimension_tables — DDL on cold-start
# ═══════════════════════════════════════════════════════════════════════════


class TestEnsureDimensionTables:
    """Verify the DDL the operator runs to create the dimension tables."""

    def test_creates_both_tables(self):
        """``_ensure_dimension_tables`` issues CREATE TABLE IF NOT EXISTS for both dims."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        _ensure_dimension_tables(mock_conn)

        assert mock_cursor.execute.call_count == 2
        executed = [c[0][0] for c in mock_cursor.execute.call_args_list]
        assert any("CREATE TABLE IF NOT EXISTS dim_geolocation" in sql for sql in executed)
        assert any("CREATE TABLE IF NOT EXISTS dim_useragent" in sql for sql in executed)
        # PK constraints and unique IPs are part of the documented schema.
        assert any("SERIAL PRIMARY KEY" in sql for sql in executed)
        assert any("ip           VARCHAR(50) UNIQUE" in sql for sql in executed)
        mock_conn.commit.assert_called_once()


# ═══════════════════════════════════════════════════════════════════════════
#  10. export_dimensions — top-level callable orchestration
# ═══════════════════════════════════════════════════════════════════════════


class TestExportDimensionsCallable:
    """Verify the top-level ``export_dimensions()`` callable orchestrates correctly."""

    def test_orchestration_order(self):
        """The three build steps execute in the documented order."""
        with patch("plugins.operators.export_dimensions._ensure_dimension_tables") as mock_ddl:
            with patch("plugins.operators.export_dimensions._ensure_default_rows") as mock_default:
                with patch("plugins.operators.export_dimensions._build_dim_geolocation") as mock_geo:
                    with patch("plugins.operators.export_dimensions._build_dim_useragent") as mock_ua:
                        with patch("plugins.operators.export_dimensions.get_conn") as mock_get_conn:
                            mock_conn = MagicMock()
                            mock_get_conn.return_value = mock_conn

                            export_dimensions()

                            mock_ddl.assert_called_once_with(mock_conn)
                            mock_default.assert_called_once_with(mock_conn)
                            mock_geo.assert_called_once_with(mock_conn)
                            mock_ua.assert_called_once_with(mock_conn)
                            mock_conn.close.assert_called_once()

    def test_conn_closed_on_error(self):
        """If a build step raises, the connection is still closed (finally block)."""
        with patch("plugins.operators.export_dimensions._build_dim_geolocation") as mock_geo:
            with patch("plugins.operators.export_dimensions.get_conn") as mock_get_conn:
                mock_conn = MagicMock()
                mock_get_conn.return_value = mock_conn
                mock_geo.side_effect = RuntimeError("Delta read failed")

                with pytest.raises(RuntimeError):
                    export_dimensions()

                mock_conn.close.assert_called_once()

    def test_callable_accepts_context(self):
        """``export_dimensions`` accepts ``**context`` (Airflow PythonOperator compat)."""
        with patch("plugins.operators.export_dimensions.get_conn") as mock_get_conn:
            mock_conn = MagicMock()
            mock_get_conn.return_value = mock_conn
            with patch("plugins.operators.export_dimensions._ensure_dimension_tables"):
                with patch("plugins.operators.export_dimensions._ensure_default_rows"):
                    with patch("plugins.operators.export_dimensions._build_dim_geolocation"):
                        with patch("plugins.operators.export_dimensions._build_dim_useragent"):
                            # Airflow passes arbitrary kwargs through to callables.
                            export_dimensions(
                                task_instance=MagicMock(),
                                execution_date="2026-05-30",
                                dag_run=MagicMock(),
                            )


# ═══════════════════════════════════════════════════════════════════════════
#  11. get_conn — connection settings
# ═══════════════════════════════════════════════════════════════════════════


class TestGetConn:
    """Verify DB connection creation reads environment variables correctly."""

    @patch("plugins.operators.export_dimensions.psycopg2.connect")
    def test_default_connection_params(self, mock_connect):
        """When no env vars are set, documented defaults are used."""
        get_conn()

        mock_connect.assert_called_once_with(
            host="postgres",
            port=5432,
            dbname="w3c_warehouse",
            user="airflow",
            password="airflow",
        )

    @patch.dict(
        "plugins.operators.export_dimensions.os.environ",
        {
            "W3C_DB_HOST": "rds.example.com",
            "W3C_DB_PORT": "5433",
            "W3C_DB_NAME": "prod_warehouse",
            "W3C_DB_USER": "admin",
            "W3C_DB_PASS": "s3cret",
        },
    )
    @patch("plugins.operators.export_dimensions.psycopg2.connect")
    def test_custom_connection_params_from_env(self, mock_connect):
        """When env vars are set, they override the defaults."""
        get_conn()

        mock_connect.assert_called_once_with(
            host="rds.example.com",
            port=5433,
            dbname="prod_warehouse",
            user="admin",
            password="s3cret",
        )


# ═══════════════════════════════════════════════════════════════════════════
#  12. Edge cases — mixed validity, real-world friction
# ═══════════════════════════════════════════════════════════════════════════


class TestEdgeCases:
    """Cross-cutting edge cases for the dimension writers."""

    def test_mixed_valid_and_invalid_ips_all_inserted(self):
        """Private / loopback / unspecified IPs from the Silver Delta
        are *not* filtered by the operator — the Spark enrichment job
        is responsible for that.  We verify the writer passes them
        through unchanged so a downstream dim FK join still resolves.
        """
        silver_df = _make_silver_df(
            ["8.8.8.8", "127.0.0.1", "0.0.0.0", "10.0.0.1"],
            country=["US", "US", "US", "US"],
            isp=["Google", "Private", "Private", "Private"],
        )
        read_conn, _ = _make_read_conn(existing_ips=[])

        with patch(
            "plugins.operators.export_dimensions._read_silver_geo_dim",
            return_value=silver_df,
        ):
            with _patched_write_conn():
                with patch("plugins.operators.export_dimensions.execute_values") as mock_exec:
                    _build_dim_geolocation(read_conn, "/silver/path")

                    insert_data = mock_exec.call_args[0][2]
                    # All four IPs — including 127.0.0.1, 0.0.0.0, 10.0.0.1 —
                    # are passed through to the INSERT payload.
                    assert len(insert_data) == 4
                    assert {row[0] for row in insert_data} == {
                        "8.8.8.8",
                        "127.0.0.1",
                        "0.0.0.0",
                        "10.0.0.1",
                    }
