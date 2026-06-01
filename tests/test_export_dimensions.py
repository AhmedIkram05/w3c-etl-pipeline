"""
Unit tests for ``export_dimensions.py`` — Airflow-managed dimension tables.

Tests the Python-level enrichment logic for the two dimension tables that
require external APIs (ip-api.com) or library parsing (user-agents):

* ``dim_geolocation`` — geo-IP resolution via ip-api.com batch API
* ``dim_useragent`` — user-agent parsing via the ``user-agents`` library

All tests use mocks throughout — no real HTTP calls or database connections.
Should run with just ``pytest``.

Usage:
    pytest tests/test_export_dimensions.py -v --tb=short
"""

import sys
from types import ModuleType
from typing import Optional
from unittest.mock import MagicMock, patch

import pytest

# ── Module-level dependency mocking ───────────────────────────────────────────
# ``export_dimensions.py`` imports ``psycopg2``, ``requests``, and
# ``user_agents`` at the module level.  We inject mocks into ``sys.modules``
# *before* any import of the module so that even pure-function imports
# (like ``_safe_int``) work without those dependencies installed.
# Tests that need real library behavior (e.g. ``_parse_user_agent``) will
# use those real libraries if available, or skip with importorskip.

_FAKE_MODULES = {
    "psycopg2": MagicMock(),
    "psycopg2.extras": MagicMock(),
}

# Store originals before patching
_ORIGINALS: dict[str, Optional[ModuleType]] = {}
for mod_name, mock in _FAKE_MODULES.items():
    if mod_name not in sys.modules:
        _ORIGINALS[mod_name] = None
        sys.modules[mod_name] = mock
    else:
        _ORIGINALS[mod_name] = sys.modules[mod_name]


# Now we can safely import from the module — the injected mocks prevent
# ImportError / ModuleNotFoundError for psycopg2, requests, user_agents.
from plugins.operators.export_dimensions import (  # noqa: E402
    _parse_user_agent,
)

# ═══════════════════════════════════════════════════════════════════════════
#  1. _parse_user_agent — pure function tests
# ═══════════════════════════════════════════════════════════════════════════


class TestParseUserAgent:
    """Unit tests for the ``_parse_user_agent()`` helper.

    These tests require the ``user-agents`` library to be installed
    (listed in tests/requirements-test.txt). If it's not available,
    the tests are skipped.
    """

    @staticmethod
    def _require_user_agents():
        """Skip test if real user_agents library is not installed.

        The mock in sys.modules provides basic support, but for meaningful
        UA parsing tests we need the real library.
        """
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
        # Pure logic in the function — also works without real user_agents
        result = _parse_user_agent("")
        assert result == {
            "AgentType": "Unknown",
            "BrowserName": "Unknown",
            "BrowserVersion": "Unknown",
            "OS": "Unknown",
            "DeviceType": "Unknown",
        }

    def test_dash_ua_returns_unknown(self):
        result = _parse_user_agent("-")
        assert result["AgentType"] == "Unknown"

    def test_unknown_string_literal_returns_unknown(self):
        result = _parse_user_agent("Unknown")
        assert result["AgentType"] == "Unknown"

    def test_none_ua(self):
        result = _parse_user_agent(None)
        assert result["AgentType"] == "Unknown"

    def test_url_encoded_ua_is_decoded(self):
        self._require_user_agents()
        ua = "Mozilla%2F5.0+%28Windows+NT+10.0%3B+Win64%3B+x64%29+AppleWebKit%2F537.36"
        result = _parse_user_agent(ua)
        assert result["BrowserName"] != "Unknown"
        assert result["AgentType"] == "Browser"

    def test_whitespace_only_ua(self):
        result = _parse_user_agent("   ")
        assert result["AgentType"] == "Unknown"


# ═══════════════════════════════════════════════════════════════════════════
#  3. Default -1 surrogate key rows
# ═══════════════════════════════════════════════════════════════════════════


class TestEnsureDefaultRows:
    """Verify the default -1 surrogate-key rows are inserted correctly."""

    # We need to re-patch get_conn because the module-level mock for
    # psycopg2 is a MagicMock that may not pass through. We explicitly
    # patch at the function level.

    @patch("plugins.operators.export_dimensions.get_conn")
    def test_dim_geolocation_default_insert_sql(self, mock_get_conn):
        """Verify the INSERT for dim_geolocation default row."""
        from plugins.operators.export_dimensions import _ensure_default_rows

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn

        _ensure_default_rows(mock_conn)

        # Should have two execute calls
        assert mock_cursor.execute.call_count == 2

        # First call: dim_geolocation default
        geoloc_sql = mock_cursor.execute.call_args_list[0][0][0]
        assert "INSERT INTO dim_geolocation" in geoloc_sql
        assert "VALUES (-1, 'Unknown', 'Unknown', 'Unknown', 'Unknown', '-', NULL, NULL, '-')" in geoloc_sql
        assert "ON CONFLICT DO NOTHING" in geoloc_sql

    @patch("plugins.operators.export_dimensions.get_conn")
    def test_dim_useragent_default_insert_sql(self, mock_get_conn):
        """Verify the INSERT for dim_useragent default row."""
        from plugins.operators.export_dimensions import _ensure_default_rows

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn

        _ensure_default_rows(mock_conn)

        # Second call: dim_useragent default
        ua_sql = mock_cursor.execute.call_args_list[1][0][0]
        assert "INSERT INTO dim_useragent" in ua_sql
        assert "VALUES (-1, 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown')" in ua_sql
        assert "ON CONFLICT DO NOTHING" in ua_sql

    @patch("plugins.operators.export_dimensions.get_conn")
    def test_ensure_default_rows_commits(self, mock_get_conn):
        """Verify the function commits the transaction."""
        from plugins.operators.export_dimensions import _ensure_default_rows

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn

        _ensure_default_rows(mock_conn)

        mock_conn.commit.assert_called_once()


# ═══════════════════════════════════════════════════════════════════════════
#  4. Private IP detection (geolocation short-circuit)
# ═══════════════════════════════════════════════════════════════════════════


class TestPrivateIPDetection:
    """Verify that private / link-local / loopback IPs skip the API call."""

    @patch("plugins.operators.export_dimensions.requests.post")
    @patch("plugins.operators.export_dimensions.get_conn")
    def test_private_10_dot_ip_no_api_call(self, mock_get_conn, mock_post):
        """10.x.x.x addresses should be short-circuited without API call."""
        mock_conn = self._mock_conn_with_new_ips(["10.0.0.1"])
        mock_get_conn.return_value = self._mock_write_conn()
        from plugins.operators.export_dimensions import _build_dim_geolocation

        _build_dim_geolocation(mock_conn)
        mock_post.assert_not_called()

    @patch("plugins.operators.export_dimensions.requests.post")
    @patch("plugins.operators.export_dimensions.get_conn")
    def test_loopback_127_dot_ip_no_api_call(self, mock_get_conn, mock_post):
        """127.x.x.x addresses should be short-circuited."""
        mock_conn = self._mock_conn_with_new_ips(["127.0.0.1"])
        mock_get_conn.return_value = self._mock_write_conn()
        from plugins.operators.export_dimensions import _build_dim_geolocation

        _build_dim_geolocation(mock_conn)
        mock_post.assert_not_called()

    @patch("plugins.operators.export_dimensions.requests.post")
    @patch("plugins.operators.export_dimensions.get_conn")
    def test_link_local_169_254_ip_no_api_call(self, mock_get_conn, mock_post):
        """169.254.x.x link-local addresses should be short-circuited."""
        mock_conn = self._mock_conn_with_new_ips(["169.254.1.1"])
        mock_get_conn.return_value = self._mock_write_conn()
        from plugins.operators.export_dimensions import _build_dim_geolocation

        _build_dim_geolocation(mock_conn)
        mock_post.assert_not_called()

    @patch("plugins.operators.export_dimensions.requests.post")
    @patch("plugins.operators.export_dimensions.get_conn")
    def test_private_192_168_ip_no_api_call(self, mock_get_conn, mock_post):
        """192.168.x.x addresses should be short-circuited."""
        mock_conn = self._mock_conn_with_new_ips(["192.168.1.100"])
        mock_get_conn.return_value = self._mock_write_conn()
        from plugins.operators.export_dimensions import _build_dim_geolocation

        _build_dim_geolocation(mock_conn)
        mock_post.assert_not_called()

    @patch("plugins.operators.export_dimensions.execute_values")
    @patch("plugins.operators.export_dimensions.requests.post")
    @patch("plugins.operators.export_dimensions.get_conn")
    def test_private_ip_inserted_with_private_geo(self, mock_get_conn, mock_post, mock_execute_values):
        """Private IPs should be inserted with Country='Private'."""
        mock_conn = self._mock_conn_with_new_ips(["192.168.1.100"])
        mock_write_conn = self._mock_write_conn()
        mock_get_conn.return_value = mock_write_conn

        from plugins.operators.export_dimensions import _build_dim_geolocation

        _build_dim_geolocation(mock_conn)

        # Verify the INSERT data includes Private geo
        insert_data = mock_execute_values.call_args[0][2]
        assert len(insert_data) == 1
        ip, country, region, *_ = insert_data[0]
        assert ip == "192.168.1.100"
        assert country == "Private"
        assert region == "Private"

    @patch("plugins.operators.export_dimensions.execute_values")
    @patch("plugins.operators.export_dimensions.requests.post")
    @patch("plugins.operators.export_dimensions.get_conn")
    def test_malformed_ip_treated_as_unknown(self, mock_get_conn, mock_post, mock_execute_values):
        """Malformed IP strings should be treated as Unknown (not crash)."""
        mock_conn = self._mock_conn_with_new_ips(["not-an-ip", "999.999.999.999"])
        mock_write_conn = self._mock_write_conn()
        mock_get_conn.return_value = mock_write_conn

        from plugins.operators.export_dimensions import _build_dim_geolocation

        _build_dim_geolocation(mock_conn)

        insert_data = mock_execute_values.call_args[0][2]
        assert len(insert_data) == 2
        for row in insert_data:
            assert row[1] == "Unknown"  # country

    def _mock_conn_with_new_ips(self, ips):
        """Create a mock connection that returns IPs as new (unresolved)."""
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_cur.fetchall.side_effect = [
            [],  # existing_ips = empty
            [(ip,) for ip in ips],  # all_ips from raw_enriched
        ]
        mock_conn.cursor.return_value = mock_cur
        return mock_conn

    def _mock_write_conn(self):
        """Create a mock connection for the write phase (INSERT)."""
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cur
        return mock_conn


# ═══════════════════════════════════════════════════════════════════════════
#  5. ip-api.com API call — payload structure
# ═══════════════════════════════════════════════════════════════════════════


class TestGeolocationAPICall:
    """Verify the ip-api.com batch API call structure."""

    @patch("plugins.operators.export_dimensions.requests.post")
    @patch("plugins.operators.export_dimensions.get_conn")
    def test_api_payload_fields(self, mock_get_conn, mock_post):
        """Verify the batch payload includes the 'fields' parameter."""
        public_ips = ["8.8.8.8", "1.1.1.1"]

        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_cur.fetchall.side_effect = [
            [],
            [(ip,) for ip in public_ips],
        ]
        mock_conn.cursor.return_value = mock_cur

        mock_response = MagicMock()
        mock_response.json.return_value = [
            {
                "query": "8.8.8.8",
                "status": "success",
                "country": "US",
                "regionName": "California",
                "city": "Mountain View",
                "zip": "94043",
                "lat": 37.386,
                "lon": -122.084,
                "isp": "Google",
            },
            {
                "query": "1.1.1.1",
                "status": "success",
                "country": "AU",
                "regionName": "Queensland",
                "city": "South Brisbane",
                "zip": "4101",
                "lat": -27.476,
                "lon": 153.017,
                "isp": "Cloudflare",
            },
        ]
        mock_post.return_value = mock_response

        mock_write_conn = MagicMock()
        mock_write_cur = MagicMock()
        mock_write_conn.cursor.return_value.__enter__.return_value = mock_write_cur
        mock_get_conn.return_value = mock_write_conn

        from plugins.operators.export_dimensions import _build_dim_geolocation

        with patch("plugins.operators.export_dimensions.execute_values"):
            _build_dim_geolocation(mock_conn)

        # Verify the API payload
        mock_post.assert_called_once()
        call_args = mock_post.call_args
        assert call_args[0][0] == "http://ip-api.com/batch"

        payload = call_args[1]["json"]
        assert len(payload) == 2
        queries = {item["query"] for item in payload}
        assert queries == {"8.8.8.8", "1.1.1.1"}
        for item in payload:
            assert "fields" in item
            assert "status" in item["fields"]

    @patch("plugins.operators.export_dimensions.requests.post")
    @patch("plugins.operators.export_dimensions.get_conn")
    def test_api_failure_sets_unknown(self, mock_get_conn, mock_post):
        """When the API call fails, IPs should be set to Unknown."""
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_cur.fetchall.side_effect = [
            [],
            [("8.8.8.8",)],
        ]
        mock_conn.cursor.return_value = mock_cur
        mock_post.side_effect = Exception("Connection timeout")

        mock_write_conn = MagicMock()
        mock_write_cur = MagicMock()
        mock_write_conn.cursor.return_value.__enter__.return_value = mock_write_cur
        mock_get_conn.return_value = mock_write_conn

        from plugins.operators.export_dimensions import _build_dim_geolocation

        with patch("plugins.operators.export_dimensions.execute_values") as mock_exec:
            _build_dim_geolocation(mock_conn)

        insert_data = mock_exec.call_args[0][2]
        assert len(insert_data) == 1
        assert insert_data[0][1] == "Unknown"


# ═══════════════════════════════════════════════════════════════════════════
#  6. Rate limit handling — sleep between batches
# ═══════════════════════════════════════════════════════════════════════════


class TestRateLimitHandling:
    """Verify 1.5s sleep between ip-api.com batch calls."""

    @patch("plugins.operators.export_dimensions.time.sleep")
    @patch("plugins.operators.export_dimensions.requests.post")
    @patch("plugins.operators.export_dimensions.get_conn")
    def test_sleep_between_batches(self, mock_get_conn, mock_post, mock_sleep):
        """When more than 100 IPs need resolution, batches are spaced 1.5s apart."""
        public_ips = [f"1.2.3.{i}" for i in range(250)]

        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_cur.fetchall.side_effect = [
            [],
            [(ip,) for ip in public_ips],
        ]
        mock_conn.cursor.return_value = mock_cur

        mock_response = MagicMock()
        mock_response.json.return_value = [
            {
                "query": ip,
                "status": "success",
                "country": "US",
                "regionName": "California",
                "city": "San Jose",
                "zip": "95134",
                "lat": 37.339,
                "lon": -121.894,
                "isp": "Test",
            }
            for ip in public_ips
        ]
        mock_post.return_value = mock_response

        mock_write_conn = MagicMock()
        mock_write_cur = MagicMock()
        mock_write_conn.cursor.return_value.__enter__.return_value = mock_write_cur
        mock_get_conn.return_value = mock_write_conn

        from plugins.operators.export_dimensions import _build_dim_geolocation

        with patch("plugins.operators.export_dimensions.execute_values"):
            _build_dim_geolocation(mock_conn)

        assert mock_post.call_count == 3
        assert mock_sleep.call_count == 2
        mock_sleep.assert_any_call(1.5)

    @patch("plugins.operators.export_dimensions.time.sleep")
    @patch("plugins.operators.export_dimensions.requests.post")
    @patch("plugins.operators.export_dimensions.get_conn")
    def test_no_sleep_for_single_batch(self, mock_get_conn, mock_post, mock_sleep):
        """When fewer than 100 IPs, no sleep is needed."""
        public_ips = [f"1.2.3.{i}" for i in range(50)]

        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_cur.fetchall.side_effect = [
            [],
            [(ip,) for ip in public_ips],
        ]
        mock_conn.cursor.return_value = mock_cur

        mock_response = MagicMock()
        mock_response.json.return_value = [
            {
                "query": ip,
                "status": "success",
                "country": "US",
                "regionName": "New York",
                "city": "New York",
                "zip": "10001",
                "lat": 40.712,
                "lon": -74.006,
                "isp": "Test",
            }
            for ip in public_ips
        ]
        mock_post.return_value = mock_response

        mock_write_conn = MagicMock()
        mock_write_cur = MagicMock()
        mock_write_conn.cursor.return_value.__enter__.return_value = mock_write_cur
        mock_get_conn.return_value = mock_write_conn

        from plugins.operators.export_dimensions import _build_dim_geolocation

        with patch("plugins.operators.export_dimensions.execute_values"):
            _build_dim_geolocation(mock_conn)

        assert mock_post.call_count == 1
        assert mock_sleep.call_count == 0


# ═══════════════════════════════════════════════════════════════════════════
#  7. INSERT ON CONFLICT DO NOTHING — SQL verification
# ═══════════════════════════════════════════════════════════════════════════


class TestInsertOnConflict:
    """Verify the INSERT SQL for both dimension tables uses ON CONFLICT."""

    @patch("plugins.operators.export_dimensions.requests.post")
    @patch("plugins.operators.export_dimensions.get_conn")
    def test_dim_geolocation_insert_sql(self, mock_get_conn, mock_post):
        """Verify dim_geolocation uses INSERT ... ON CONFLICT (ip) DO NOTHING."""
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_cur.fetchall.side_effect = [
            [],
            [("8.8.8.8",)],
        ]
        mock_conn.cursor.return_value = mock_cur

        mock_response = MagicMock()
        mock_response.json.return_value = [
            {
                "query": "8.8.8.8",
                "status": "success",
                "country": "US",
                "regionName": "California",
                "city": "Mountain View",
                "zip": "94043",
                "lat": 37.386,
                "lon": -122.084,
                "isp": "Google",
            },
        ]
        mock_post.return_value = mock_response

        mock_write_conn = MagicMock()
        mock_write_cur = MagicMock()
        mock_write_conn.cursor.return_value.__enter__.return_value = mock_write_cur
        mock_get_conn.return_value = mock_write_conn

        from plugins.operators.export_dimensions import _build_dim_geolocation

        with patch("plugins.operators.export_dimensions.execute_values") as mock_execute_values:
            _build_dim_geolocation(mock_conn)

            insert_sql = mock_execute_values.call_args[0][1]
            assert "INSERT INTO dim_geolocation" in insert_sql
            assert "ON CONFLICT (ip) DO NOTHING" in insert_sql

    @patch("plugins.operators.export_dimensions.get_conn")
    def test_dim_useragent_insert_sql(self, mock_get_conn):
        """Verify dim_useragent uses INSERT ... ON CONFLICT (user_agent) DO NOTHING."""
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_cur.fetchall.return_value = [("Mozilla/5.0 Chrome/120",)]
        mock_conn.cursor.return_value = mock_cur

        mock_write_conn = MagicMock()
        mock_write_cur = MagicMock()
        mock_write_conn.cursor.return_value.__enter__.return_value = mock_write_cur
        mock_get_conn.return_value = mock_write_conn

        from plugins.operators.export_dimensions import _build_dim_useragent

        with patch("plugins.operators.export_dimensions.execute_values") as mock_exec:
            _build_dim_useragent(mock_conn)

            insert_sql = mock_exec.call_args[0][1]
            assert "INSERT INTO dim_useragent" in insert_sql
            assert "ON CONFLICT (user_agent) DO NOTHING" in insert_sql


# ═══════════════════════════════════════════════════════════════════════════
#  8. SQL extraction — SELECT DISTINCT logic
# ═══════════════════════════════════════════════════════════════════════════


class TestSQLQueries:
    """Verify the SELECT DISTINCT queries used to extract source data."""

    @patch("plugins.operators.export_dimensions.requests.post")
    @patch("plugins.operators.export_dimensions.get_conn")
    def test_dim_geolocation_selects_distinct_client_ip(self, mock_get_conn, mock_post):
        """Verify _build_dim_geolocation queries distinct client_ip from raw_enriched."""
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_cur.fetchall.side_effect = [
            [],
            [("8.8.8.8",)],
        ]
        mock_conn.cursor.return_value = mock_cur

        mock_write_conn = MagicMock()
        mock_write_cur = MagicMock()
        mock_write_conn.cursor.return_value.__enter__.return_value = mock_write_cur
        mock_get_conn.return_value = mock_write_conn

        mock_post.return_value.json.return_value = []

        from plugins.operators.export_dimensions import _build_dim_geolocation

        with patch("plugins.operators.export_dimensions.execute_values"):
            _build_dim_geolocation(mock_conn)

        select_calls = [c[0][0] for c in mock_cur.execute.call_args_list]
        assert any("SELECT DISTINCT client_ip FROM raw_enriched" in sql for sql in select_calls)
        assert any("client_ip IS NOT NULL" in sql for sql in select_calls)
        assert any("client_ip != '-'" in sql for sql in select_calls)

    @patch("plugins.operators.export_dimensions.get_conn")
    def test_dim_useragent_selects_distinct_user_agent(self, mock_get_conn):
        """Verify _build_dim_useragent queries distinct user_agent from raw_enriched."""
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_cur.fetchall.return_value = []
        mock_conn.cursor.return_value = mock_cur

        mock_write_conn = MagicMock()
        mock_write_cur = MagicMock()
        mock_write_conn.cursor.return_value.__enter__.return_value = mock_write_cur
        mock_get_conn.return_value = mock_write_conn

        from plugins.operators.export_dimensions import _build_dim_useragent

        _build_dim_useragent(mock_conn)

        select_calls = [c[0][0] for c in mock_cur.execute.call_args_list]
        assert any("SELECT DISTINCT user_agent FROM raw_enriched" in sql for sql in select_calls)
        assert any("user_agent IS NOT NULL" in sql for sql in select_calls)
        assert any("user_agent != '-'" in sql for sql in select_calls)

    @patch("plugins.operators.export_dimensions.requests.post")
    @patch("plugins.operators.export_dimensions.get_conn")
    def test_geolocation_checks_existing_ips_first(self, mock_get_conn, mock_post):
        """Verify _build_dim_geolocation first checks which IPs already resolved."""
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_cur.fetchall.side_effect = [
            [("8.8.8.8",)],  # already resolved
            [],  # no new IPs
        ]
        mock_conn.cursor.return_value = mock_cur

        mock_write_conn = MagicMock()
        mock_write_cur = MagicMock()
        mock_write_conn.cursor.return_value.__enter__.return_value = mock_write_cur
        mock_get_conn.return_value = mock_write_conn

        from plugins.operators.export_dimensions import _build_dim_geolocation

        _build_dim_geolocation(mock_conn)

        select_calls = [c[0][0] for c in mock_cur.execute.call_args_list]
        assert any("SELECT DISTINCT ip FROM dim_geolocation" in sql for sql in select_calls)


# ═══════════════════════════════════════════════════════════════════════════
#  9. _build_dim_useragent — edge cases
# ═══════════════════════════════════════════════════════════════════════════


class TestBuildDimUserAgentEdgeCases:
    """Verify edge cases in the user-agent dimension builder."""

    def test_no_rows_in_raw_enriched(self):
        """When raw_enriched has no rows, _build_dim_useragent does nothing."""
        from plugins.operators.export_dimensions import _build_dim_useragent

        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_cur.fetchall.return_value = []
        mock_conn.cursor.return_value = mock_cur

        _build_dim_useragent(mock_conn)

        # Only the read cursor should have been created (no write connection)
        mock_conn.cursor.assert_called_once()

    @patch("plugins.operators.export_dimensions.get_conn")
    def test_ua_truncated_to_1000_chars(self, mock_get_conn):
        """Very long user-agent strings should be truncated to 1000 chars."""
        from plugins.operators.export_dimensions import _build_dim_useragent

        mock_conn = MagicMock()
        mock_cur = MagicMock()
        long_ua = "Mozilla/" + "X" * 2000
        mock_cur.fetchall.return_value = [(long_ua,)]
        mock_conn.cursor.return_value = mock_cur

        mock_write_conn = MagicMock()
        mock_write_cur = MagicMock()
        mock_write_conn.cursor.return_value.__enter__.return_value = mock_write_cur
        mock_get_conn.return_value = mock_write_conn

        with patch("plugins.operators.export_dimensions.execute_values") as mock_exec:
            _build_dim_useragent(mock_conn)
            insert_data = mock_exec.call_args[0][2]
            ua_value = insert_data[0][0]
            assert len(ua_value) == 1000


# ═══════════════════════════════════════════════════════════════════════════
#  10. export_dimensions — top-level callable orchestration
# ═══════════════════════════════════════════════════════════════════════════


class TestExportDimensionsCallable:
    """Verify the top-level ``export_dimensions()`` callable orchestrates correctly."""

    @patch("plugins.operators.export_dimensions._build_dim_useragent")
    @patch("plugins.operators.export_dimensions._build_dim_geolocation")
    @patch("plugins.operators.export_dimensions._ensure_default_rows")
    @patch("plugins.operators.export_dimensions.get_conn")
    def test_orchestration_order(self, mock_get_conn, mock_default, mock_geo, mock_ua):
        """Verify the three build steps execute in the correct order."""
        from plugins.operators.export_dimensions import export_dimensions

        mock_conn = MagicMock()
        mock_get_conn.return_value = mock_conn

        export_dimensions()

        mock_default.assert_called_once_with(mock_conn)
        mock_geo.assert_called_once_with(mock_conn)
        mock_ua.assert_called_once_with(mock_conn)
        mock_conn.close.assert_called_once()

    @patch("plugins.operators.export_dimensions._build_dim_useragent")
    @patch("plugins.operators.export_dimensions._build_dim_geolocation")
    @patch("plugins.operators.export_dimensions._ensure_default_rows")
    @patch("plugins.operators.export_dimensions.get_conn")
    def test_conn_closed_on_error(self, mock_get_conn, mock_default, mock_geo, mock_ua):
        """If a build step raises, the connection is still closed (finally block)."""
        from plugins.operators.export_dimensions import export_dimensions

        mock_conn = MagicMock()
        mock_get_conn.return_value = mock_conn
        mock_geo.side_effect = RuntimeError("API failure")

        with pytest.raises(RuntimeError):
            export_dimensions()

        mock_conn.close.assert_called_once()

    def test_callable_accepts_context(self):
        """Verify export_dimensions accepts **context (Airflow PythonOperator compat)."""
        from plugins.operators.export_dimensions import export_dimensions

        with patch("plugins.operators.export_dimensions.get_conn") as mock_get_conn:
            mock_conn = MagicMock()
            mock_get_conn.return_value = mock_conn

            with patch("plugins.operators.export_dimensions._ensure_default_rows"):
                with patch("plugins.operators.export_dimensions._build_dim_geolocation"):
                    with patch("plugins.operators.export_dimensions._build_dim_useragent"):
                        export_dimensions(
                            task_instance=MagicMock(),
                            execution_date="2026-05-30",
                        )


# ═══════════════════════════════════════════════════════════════════════════
#  11. get_conn — connection settings
# ═══════════════════════════════════════════════════════════════════════════


class TestGetConn:
    """Verify DB connection creation reads environment variables correctly."""

    @patch("plugins.operators.export_dimensions.psycopg2.connect")
    def test_default_connection_params(self, mock_connect):
        """When no env vars are set, defaults are used."""
        from plugins.operators.export_dimensions import get_conn

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
        from plugins.operators.export_dimensions import get_conn

        get_conn()

        mock_connect.assert_called_once_with(
            host="rds.example.com",
            port=5433,
            dbname="prod_warehouse",
            user="admin",
            password="s3cret",
        )
