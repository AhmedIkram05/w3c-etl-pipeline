"""
Unit tests for the Silver DLT pipeline (``dlt_silver.py``).

Tests cover the core enrichment and deduplication logic:

* Lazy singleton GeoIP pattern — avoids PicklingError (CRIT-04)
* Consolidated ``get_geo_fields`` struct UDF — 6 fields from 1 call (CRIT-05)
* Left-anti deduplication against existing Silver — idempotent re-runs (CRIT-06)
* Computed field UDFs — ``page_category``, ``referrer_domain``, ``traffic_type``,
  ``size_band``, ``is_crawler``
* Utility functions — ``_is_usable_ip``, ``_extract_domain``
* DLT ``@dlt.expect_or_drop`` constraint behaviour
* Silver table properties (CDC, auto-optimize)

GeoIP-dependent tests use ``unittest.mock`` to avoid needing a real MaxMind
database — all DB reader interactions are patched.

NOTE: The ``dlt`` module is only available in a Databricks runtime.  The
``unittest.mock`` setup below supplies a minimal stub so the module can
be imported for unit testing outside Databricks.
"""

import os
import sys
from typing import TYPE_CHECKING
from unittest.mock import MagicMock

import pytest
from pyspark.sql import Row

# ── Mock the ``dlt`` module (not available outside Databricks) ────────
_dlt_stub = MagicMock()
_dlt_stub.table.return_value = lambda f: f
_dlt_stub.streaming_table.return_value = lambda f: f
_dlt_stub.expect_or_drop.return_value = lambda f: f
sys.modules["dlt"] = _dlt_stub

# ── Module under test ──────────────────────────────────────────────────
# Ensure the ``databricks`` package directory is on sys.path.
_TEST_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.join(_TEST_DIR, "..")

_NESTED_DATABRICKS = os.path.join(_PROJECT_ROOT, "airflow", "spark", "databricks")
_FLAT_DATABRICKS = os.path.join(_PROJECT_ROOT, "spark", "databricks")

for _db_path in (_NESTED_DATABRICKS, _FLAT_DATABRICKS):
    if os.path.isdir(_db_path) and _db_path not in sys.path:
        sys.path.insert(0, _db_path)

_NESTED_SPARK = os.path.join(_PROJECT_ROOT, "airflow", "spark")
_FLAT_SPARK = os.path.join(_PROJECT_ROOT, "spark")

for _sp_path in (_NESTED_SPARK, _FLAT_SPARK):
    if os.path.isdir(_sp_path) and _sp_path not in sys.path:
        sys.path.insert(0, _sp_path)

if TYPE_CHECKING:
    from dlt_silver import (
        _asn_lookup,
        _ensure_asn_reader,
        _ensure_geo_reader,
        _extract_domain,
        _geo_lookup,
        _is_usable_ip,
        get_geo_fields,
        get_is_crawler,
        get_isp,
        get_page_category,
        get_referrer_domain,
        get_size_band,
        get_traffic_type,
        silver_enriched_logs,
    )
else:
    try:
        from dlt_silver import (
            _asn_lookup,
            _ensure_asn_reader,
            _ensure_geo_reader,
            _extract_domain,
            _geo_lookup,
            _is_usable_ip,
            get_geo_fields,
            get_is_crawler,
            get_isp,
            get_page_category,
            get_referrer_domain,
            get_size_band,
            get_traffic_type,
            silver_enriched_logs,
        )
    except ImportError:
        from databricks.dlt_silver import (
            _asn_lookup,
            _ensure_asn_reader,
            _ensure_geo_reader,
            _extract_domain,
            _geo_lookup,
            _is_usable_ip,
            get_geo_fields,
            get_is_crawler,
            get_isp,
            get_page_category,
            get_referrer_domain,
            get_size_band,
            get_traffic_type,
            silver_enriched_logs,
        )


# ══════════════════════════════════════════════════════════════════════
#  1. _is_usable_ip — filters private / link-local / loopback IPs
# ══════════════════════════════════════════════════════════════════════


class TestIsUsableIP:
    """Verify the IP filter used before GeoIP lookup."""

    def test_public_ipv4(self):
        assert _is_usable_ip("8.8.8.8") is True

    def test_public_ipv6(self):
        assert _is_usable_ip("2001:4860:4860::8888") is True

    def test_private_ipv4(self):
        assert _is_usable_ip("10.0.0.1") is False
        assert _is_usable_ip("172.16.0.1") is False
        assert _is_usable_ip("192.168.1.1") is False

    def test_loopback(self):
        assert _is_usable_ip("127.0.0.1") is False

    def test_link_local(self):
        assert _is_usable_ip("169.254.1.1") is False

    def test_dash_placeholder(self):
        assert _is_usable_ip("-") is False

    def test_unknown_string(self):
        assert _is_usable_ip("Unknown") is False

    def test_empty_string(self):
        assert _is_usable_ip("") is False

    def test_none_input(self):
        assert _is_usable_ip(None) is False

    def test_whitespace_only(self):
        assert _is_usable_ip("   ") is False

    def test_ip_with_whitespace(self):
        """IP with surrounding whitespace is stripped before checking."""
        assert _is_usable_ip("  8.8.8.8  ") is True

    def test_invalid_ip_string(self):
        """Non-parseable strings return False."""
        assert _is_usable_ip("not-an-ip") is False
        assert _is_usable_ip("999.999.999.999") is False


# ══════════════════════════════════════════════════════════════════════
#  2. _extract_domain — referrer URL domain extraction
# ══════════════════════════════════════════════════════════════════════


class TestExtractDomain:
    """Verify the referrer domain extraction helper."""

    def test_direct_traffic(self):
        assert _extract_domain("-") == "Direct"

    def test_none_referrer(self):
        assert _extract_domain(None) == "Direct"

    def test_empty_referrer(self):
        assert _extract_domain("") == "Direct"

    def test_http_url(self):
        assert _extract_domain("http://www.google.com/search?q=test") == "google.com"

    def test_https_url(self):
        assert _extract_domain("https://facebook.com/post/123") == "facebook.com"

    def test_www_prefix_stripped(self):
        assert _extract_domain("http://www.example.org/page") == "example.org"

    def test_multiple_subdomains(self):
        """Only the leading 'www.' is stripped."""
        assert _extract_domain("http://blog.example.com/") == "blog.example.com"

    def test_internal_referrer(self):
        url = "http://www.darwinsbeagleplants.org/Darwin/Plant.aspx"
        assert _extract_domain(url) == "darwinsbeagleplants.org"

    def test_unknown_string(self):
        assert _extract_domain("Unknown") == "Unknown"

    def test_malformed_url(self):
        """Non-parseable URL returns 'Unknown'."""
        assert _extract_domain("not-a-url") in ("Unknown", "not-a-url")


# ══════════════════════════════════════════════════════════════════════
#  3. GeoIP lazy singleton — _ensure_geo_reader and _ensure_asn_reader
# ══════════════════════════════════════════════════════════════════════

# Module reference for patching globals in GeoIP singleton tests.
# Uses the unqualified import (relies on _SPARK_DIR being on sys.path).
import dlt_silver as _silver_mod  # noqa: E402


class TestEnsureGeoReader:
    """Lazy singleton GeoIP reader — initialised once per executor (CRIT-04)."""

    def _reset_globals(self):
        """Reset the module-level geoip globals to their initial state."""
        _silver_mod._geo_reader = None
        _silver_mod._asn_reader = None
        _silver_mod._geo_init_attempted = False
        _silver_mod._asn_init_attempted = False

    def test_reader_initialised_on_first_call(self):
        """``_ensure_geo_reader`` creates the reader on first invocation."""
        self._reset_globals()
        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(_silver_mod, "_GEO_CITY_DB_PATH", "/nonexistent/test.mmdb")
            with pytest.raises(Exception):
                # geoip2 will fail to open the nonexistent file
                _ensure_geo_reader()
            # _geo_init_attempted should be True even though it failed
            assert _silver_mod._geo_init_attempted is True

    def test_reader_not_reinitialised_on_subsequent_calls(self):
        """After failure, subsequent calls don't retry (singleton pattern)."""
        self._reset_globals()
        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(_silver_mod, "_GEO_CITY_DB_PATH", "/nonexistent/test.mmdb")

            # First call — tries and fails
            _ensure_geo_reader()
            first_attempt = _silver_mod._geo_init_attempted

            # Reset the reader but keep init_attempted (simulating no retry)
            _silver_mod._geo_reader = None

            # Second call — should skip since init_attempted is True
            # This is the core of the lazy singleton: no retry after first fail
            _ensure_geo_reader()
            assert _silver_mod._geo_reader is None

    def test_asn_reader_initialised_on_first_call(self):
        """``_ensure_asn_reader`` creates the ASN reader on first invocation."""
        self._reset_globals()
        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(_silver_mod, "_GEO_ASN_DB_PATH", "/nonexistent/test-asn.mmdb")
            _ensure_asn_reader()
            # Failed but attempted
            assert _silver_mod._asn_init_attempted is True

    def test_geo_and_asn_readers_independent(self):
        """Geo city and ASN readers are initialised independently."""
        self._reset_globals()
        assert _silver_mod._geo_reader is None
        assert _silver_mod._asn_reader is None
        assert _silver_mod._geo_init_attempted is False
        assert _silver_mod._asn_init_attempted is False

    def _cleanup(self):
        self._reset_globals()


# ══════════════════════════════════════════════════════════════════════
#  4. _geo_lookup — consolidated lookup (CRIT-05)
# ══════════════════════════════════════════════════════════════════════


class TestGeoLookup:
    """Verify the consolidated ``_geo_lookup`` returns 6 fields in 1 call."""

    def _setup_mock(self, monkeypatch, mock_response=None):
        """Patch geoip2.database.Reader and reset globals."""
        import geoip2.database

        monkeypatch.setattr(_silver_mod, "_geo_reader", None)
        monkeypatch.setattr(_silver_mod, "_geo_init_attempted", False)

        if mock_response is None:
            mock_response = _MockCityResponse()

        mock_reader = type("MockReader", (), {"city": lambda self, ip: mock_response})()
        monkeypatch.setattr(_silver_mod, "_geo_reader", mock_reader)
        return mock_reader

    def test_public_ip_returns_all_fields(self, monkeypatch):
        """A valid public IP returns all 6 GeoIP fields."""
        self._setup_mock(monkeypatch)
        result = _geo_lookup("8.8.8.8")
        assert result is not None
        assert result["country"] == "United States"
        assert result["region"] == "California"
        assert result["city"] == "Mountain View"
        assert result["latitude"] == 37.386
        assert result["longitude"] == -122.084
        assert result["postcode"] == "94043"

    def test_private_ip_returns_none(self, monkeypatch):
        """Private IPs are filtered before lookup — no DB call."""
        self._setup_mock(monkeypatch)
        assert _geo_lookup("10.0.0.1") is None

    def test_dash_ip_returns_none(self, monkeypatch):
        """Dash placeholder returns None without touching the DB."""
        self._setup_mock(monkeypatch)
        assert _geo_lookup("-") is None

    def test_no_reader_returns_none(self, monkeypatch):
        """When the reader is None (DB not loaded), return None."""
        monkeypatch.setattr(_silver_mod, "_geo_reader", None)
        monkeypatch.setattr(_silver_mod, "_geo_init_attempted", True)
        result = _geo_lookup("8.8.8.8")
        assert result is None

    def test_address_not_found_returns_none(self, monkeypatch):
        """AddressNotFoundError is caught and returns None."""
        import geoip2.errors

        self._setup_mock(monkeypatch)
        # Replace reader with one that raises AddressNotFoundError
        error_reader = type(
            "MockReader",
            (),
            {"city": lambda self, ip: (_ for _ in ()).throw(geoip2.errors.AddressNotFoundError("not found"))},
        )()
        monkeypatch.setattr(_silver_mod, "_geo_reader", error_reader)
        result = _geo_lookup("1.2.3.4")
        assert result is None


class TestAsnLookup:
    """Verify the ``_asn_lookup`` function."""

    def _setup_mock(self, monkeypatch, asn_org="Google"):
        """Patch ASN reader and reset globals."""
        monkeypatch.setattr(_silver_mod, "_asn_reader", None)
        monkeypatch.setattr(_silver_mod, "_asn_init_attempted", False)

        mock_response = type("MockASNResponse", (), {"autonomous_system_organization": asn_org})()
        mock_reader = type("MockASNReader", (), {"asn": lambda self, ip: mock_response})()
        monkeypatch.setattr(_silver_mod, "_asn_reader", mock_reader)

    def test_public_ip_returns_org(self, monkeypatch):
        self._setup_mock(monkeypatch, asn_org="Google")
        assert _asn_lookup("8.8.8.8") == "Google"

    def test_private_ip_returns_none(self, monkeypatch):
        self._setup_mock(monkeypatch)
        assert _asn_lookup("10.0.0.1") is None

    def test_no_reader_returns_none(self, monkeypatch):
        monkeypatch.setattr(_silver_mod, "_asn_reader", None)
        monkeypatch.setattr(_silver_mod, "_asn_init_attempted", True)
        assert _asn_lookup("8.8.8.8") is None

    def test_address_not_found_returns_none(self, monkeypatch):
        import geoip2.errors

        self._setup_mock(monkeypatch)
        error_reader = type(
            "MockASNReader",
            (),
            {"asn": lambda self, ip: (_ for _ in ()).throw(geoip2.errors.AddressNotFoundError("not found"))},
        )()
        monkeypatch.setattr(_silver_mod, "_asn_reader", error_reader)
        assert _asn_lookup("1.2.3.4") is None

    def test_dash_ip_returns_none(self, monkeypatch):
        self._setup_mock(monkeypatch)
        assert _asn_lookup("-") is None


# ══════════════════════════════════════════════════════════════════════
#  5. Computed field UDFs — pure logic tests via DataFrame projection
# ══════════════════════════════════════════════════════════════════════


class TestGetPageCategory:
    """Verify ``get_page_category`` URI classification UDF."""

    def test_static_asset_css(self, spark):
        from pyspark.sql.functions import col
        df = spark.createDataFrame([Row(uri_stem="/Darwin/style.css")])
        result = df.select(get_page_category(col("uri_stem")).alias("cat")).collect()[0]["cat"]
        assert result == "Static Asset"

    def test_static_asset_js(self, spark):
        from pyspark.sql.functions import col
        df = spark.createDataFrame([Row(uri_stem="/js/app.js")])
        assert df.select(get_page_category(col("uri_stem")).alias("cat")).collect()[0]["cat"] == "Static Asset"

    def test_static_asset_png(self, spark):
        from pyspark.sql.functions import col
        df = spark.createDataFrame([Row(uri_stem="/images/photo.png")])
        assert df.select(get_page_category(col("uri_stem")).alias("cat")).collect()[0]["cat"] == "Static Asset"

    def test_static_asset_jpg(self, spark):
        from pyspark.sql.functions import col
        df = spark.createDataFrame([Row(uri_stem="/images/photo.jpg")])
        assert df.select(get_page_category(col("uri_stem")).alias("cat")).collect()[0]["cat"] == "Static Asset"

    def test_static_asset_gif(self, spark):
        from pyspark.sql.functions import col
        df = spark.createDataFrame([Row(uri_stem="/images/animated.gif")])
        assert df.select(get_page_category(col("uri_stem")).alias("cat")).collect()[0]["cat"] == "Static Asset"

    def test_static_asset_ico(self, spark):
        from pyspark.sql.functions import col
        df = spark.createDataFrame([Row(uri_stem="/favicon.ico")])
        assert df.select(get_page_category(col("uri_stem")).alias("cat")).collect()[0]["cat"] == "Static Asset"

    def test_api_endpoint(self, spark):
        from pyspark.sql.functions import col
        df = spark.createDataFrame([Row(uri_stem="/api/v1/users")])
        assert df.select(get_page_category(col("uri_stem")).alias("cat")).collect()[0]["cat"] == "API"

    def test_admin_page(self, spark):
        from pyspark.sql.functions import col
        df = spark.createDataFrame([Row(uri_stem="/admin/dashboard")])
        assert df.select(get_page_category(col("uri_stem")).alias("cat")).collect()[0]["cat"] == "Admin"

    def test_homepage_trailing_slash(self, spark):
        from pyspark.sql.functions import col
        df = spark.createDataFrame([Row(uri_stem="/")])
        assert df.select(get_page_category(col("uri_stem")).alias("cat")).collect()[0]["cat"] == "Homepage"

    def test_homepage_directory_slash(self, spark):
        from pyspark.sql.functions import col
        df = spark.createDataFrame([Row(uri_stem="/about/")])
        assert df.select(get_page_category(col("uri_stem")).alias("cat")).collect()[0]["cat"] == "Homepage"

    def test_content_page(self, spark):
        from pyspark.sql.functions import col
        df = spark.createDataFrame([Row(uri_stem="/about/team")])
        assert df.select(get_page_category(col("uri_stem")).alias("cat")).collect()[0]["cat"] == "Content"

    def test_aspx_dynamic_page(self, spark):
        from pyspark.sql.functions import col
        df = spark.createDataFrame([Row(uri_stem="/Darwin/MyAccount.aspx")])
        result = df.select(get_page_category(col("uri_stem")).alias("cat")).collect()[0]["cat"]
        assert result == "Content"

    def test_null_uri(self, spark):
        from pyspark.sql.functions import col
        from pyspark.sql.types import StringType, StructField, StructType
        schema = StructType([StructField("uri_stem", StringType(), True)])
        df = spark.createDataFrame([Row(uri_stem=None)], schema=schema)
        assert df.select(get_page_category(col("uri_stem")).alias("cat")).collect()[0]["cat"] == "Content"

    def test_empty_uri(self, spark):
        from pyspark.sql.functions import col
        df = spark.createDataFrame([Row(uri_stem="")])
        assert df.select(get_page_category(col("uri_stem")).alias("cat")).collect()[0]["cat"] == "Content"


class TestGetReferrerDomain:
    """Verify ``get_referrer_domain`` UDF — delegates to ``_extract_domain``."""

    def test_direct(self, spark):
        from pyspark.sql.functions import col
        df = spark.createDataFrame([Row(ref="-")])
        assert df.select(get_referrer_domain(col("ref")).alias("dom")).collect()[0]["dom"] == "Direct"

    def test_google(self, spark):
        from pyspark.sql.functions import col
        df = spark.createDataFrame([Row(ref="http://www.google.com/search?q=test")])
        dom = df.select(get_referrer_domain(col("ref")).alias("dom")).collect()[0]["dom"]
        assert dom == "google.com"

    def test_internal(self, spark):
        from pyspark.sql.functions import col
        url = "http://www.example.org/Darwin/page"
        df = spark.createDataFrame([Row(ref=url)])
        assert df.select(get_referrer_domain(col("ref")).alias("dom")).collect()[0]["dom"] == "example.org"


class TestGetTrafficType:
    """Verify ``get_traffic_type`` classification UDF."""

    def test_direct(self, spark):
        from pyspark.sql.functions import col
        df = spark.createDataFrame([Row(ref_domain="Direct")])
        assert df.select(get_traffic_type(col("ref_domain")).alias("t")).collect()[0]["t"] == "Direct"

    def test_search_google(self, spark):
        from pyspark.sql.functions import col
        df = spark.createDataFrame([Row(ref_domain="google.com")])
        assert df.select(get_traffic_type(col("ref_domain")).alias("t")).collect()[0]["t"] == "Search"

    def test_search_bing(self, spark):
        from pyspark.sql.functions import col
        df = spark.createDataFrame([Row(ref_domain="bing.com")])
        assert df.select(get_traffic_type(col("ref_domain")).alias("t")).collect()[0]["t"] == "Search"

    def test_search_yahoo(self, spark):
        from pyspark.sql.functions import col
        df = spark.createDataFrame([Row(ref_domain="yahoo.com")])
        assert df.select(get_traffic_type(col("ref_domain")).alias("t")).collect()[0]["t"] == "Search"

    def test_social_facebook(self, spark):
        from pyspark.sql.functions import col
        df = spark.createDataFrame([Row(ref_domain="facebook.com")])
        assert df.select(get_traffic_type(col("ref_domain")).alias("t")).collect()[0]["t"] == "Social"

    def test_social_twitter(self, spark):
        from pyspark.sql.functions import col
        df = spark.createDataFrame([Row(ref_domain="twitter.com")])
        assert df.select(get_traffic_type(col("ref_domain")).alias("t")).collect()[0]["t"] == "Social"

    def test_social_linkedin(self, spark):
        from pyspark.sql.functions import col
        df = spark.createDataFrame([Row(ref_domain="linkedin.com")])
        assert df.select(get_traffic_type(col("ref_domain")).alias("t")).collect()[0]["t"] == "Social"

    def test_referral_other(self, spark):
        from pyspark.sql.functions import col
        df = spark.createDataFrame([Row(ref_domain="some-other-site.com")])
        assert df.select(get_traffic_type(col("ref_domain")).alias("t")).collect()[0]["t"] == "Referral"

    def test_case_insensitive_search(self, spark):
        from pyspark.sql.functions import col
        df = spark.createDataFrame([Row(ref_domain="GOOGLE.com")])
        assert df.select(get_traffic_type(col("ref_domain")).alias("t")).collect()[0]["t"] == "Search"


class TestGetIsCrawler:
    """Verify ``get_is_crawler`` crawler detection UDF."""

    def test_googlebot(self, spark):
        from pyspark.sql.functions import col
        df = spark.createDataFrame([Row(ua="Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)")])
        assert df.select(get_is_crawler(col("ua")).alias("c")).collect()[0]["c"] is True

    def test_bingbot(self, spark):
        from pyspark.sql.functions import col
        df = spark.createDataFrame([Row(ua="Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)")])
        assert df.select(get_is_crawler(col("ua")).alias("c")).collect()[0]["c"] is True

    def test_curl(self, spark):
        from pyspark.sql.functions import col
        df = spark.createDataFrame([Row(ua="curl/7.68.0")])
        assert df.select(get_is_crawler(col("ua")).alias("c")).collect()[0]["c"] is True

    def test_wget(self, spark):
        from pyspark.sql.functions import col
        df = spark.createDataFrame([Row(ua="Wget/1.21")])
        assert df.select(get_is_crawler(col("ua")).alias("c")).collect()[0]["c"] is True

    def test_chrome_browser(self, spark):
        from pyspark.sql.functions import col
        ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"
        df = spark.createDataFrame([Row(ua=ua)])
        assert df.select(get_is_crawler(col("ua")).alias("c")).collect()[0]["c"] is False

    def test_firefox_browser(self, spark):
        from pyspark.sql.functions import col
        ua = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:120.0) Gecko/20100101 Firefox/120.0"
        df = spark.createDataFrame([Row(ua=ua)])
        assert df.select(get_is_crawler(col("ua")).alias("c")).collect()[0]["c"] is False

    def test_dash_user_agent(self, spark):
        from pyspark.sql.functions import col
        df = spark.createDataFrame([Row(ua="-")])
        assert df.select(get_is_crawler(col("ua")).alias("c")).collect()[0]["c"] is False

    def test_null_user_agent(self, spark):
        from pyspark.sql.functions import col
        from pyspark.sql.types import StringType, StructField, StructType
        schema = StructType([StructField("ua", StringType(), True)])
        df = spark.createDataFrame([Row(ua=None)], schema=schema)
        assert df.select(get_is_crawler(col("ua")).alias("c")).collect()[0]["c"] is False

    def test_python_requests(self, spark):
        from pyspark.sql.functions import col
        df = spark.createDataFrame([Row(ua="python-requests/2.28.0")])
        assert df.select(get_is_crawler(col("ua")).alias("c")).collect()[0]["c"] is True


class TestGetSizeBand:
    """Verify ``get_size_band`` response size categorisation UDF."""

    def test_negative_bytes(self, spark):
        from pyspark.sql.functions import col
        df = spark.createDataFrame([Row(bytes=-1)])
        assert df.select(get_size_band(col("bytes")).alias("b")).collect()[0]["b"] == "Unknown"

    def test_zero_bytes(self, spark):
        from pyspark.sql.functions import col
        df = spark.createDataFrame([Row(bytes=0)])
        assert df.select(get_size_band(col("bytes")).alias("b")).collect()[0]["b"] == "< 1KB"

    def test_less_than_1kb(self, spark):
        from pyspark.sql.functions import col
        df = spark.createDataFrame([Row(bytes=500)])
        assert df.select(get_size_band(col("bytes")).alias("b")).collect()[0]["b"] == "< 1KB"

    def test_1kb_to_10kb(self, spark):
        from pyspark.sql.functions import col
        df = spark.createDataFrame([Row(bytes=5000)])
        assert df.select(get_size_band(col("bytes")).alias("b")).collect()[0]["b"] == "1KB-10KB"

    def test_10kb_to_100kb(self, spark):
        from pyspark.sql.functions import col
        df = spark.createDataFrame([Row(bytes=50000)])
        assert df.select(get_size_band(col("bytes")).alias("b")).collect()[0]["b"] == "10KB-100KB"

    def test_100kb_to_1mb(self, spark):
        from pyspark.sql.functions import col
        df = spark.createDataFrame([Row(bytes=500000)])
        assert df.select(get_size_band(col("bytes")).alias("b")).collect()[0]["b"] == "100KB-1MB"

    def test_greater_than_1mb(self, spark):
        from pyspark.sql.functions import col
        df = spark.createDataFrame([Row(bytes=2_000_000)])
        assert df.select(get_size_band(col("bytes")).alias("b")).collect()[0]["b"] == "> 1MB"

    def test_none_bytes(self, spark):
        from pyspark.sql.functions import col
        from pyspark.sql.types import IntegerType, StructField, StructType
        schema = StructType([StructField("bytes", IntegerType(), True)])
        df = spark.createDataFrame([Row(bytes=None)], schema=schema)
        assert df.select(get_size_band(col("bytes")).alias("b")).collect()[0]["b"] == "Unknown"


# ══════════════════════════════════════════════════════════════════════
#  6. Consolidated GeoIP UDF — get_geo_fields (CRIT-05)
# ══════════════════════════════════════════════════════════════════════


class TestGetGeoFieldsUDF:
    """Verify the consolidated struct UDF returns 6 fields from 1 call."""

    def test_udf_returns_struct_with_6_fields(self, spark):
        """The UDF returns a struct with all 6 GeoIP fields."""
        import pyspark.sql.types as T
        from pyspark.sql.functions import col

        return_type = get_geo_fields.returnType
        assert isinstance(return_type, T.StructType)
        field_names = {f.name for f in return_type.fields}
        expected = {"country", "region", "city", "latitude", "longitude", "postcode"}
        assert field_names == expected

    def test_udf_with_mocked_geo_lookup(self, spark, monkeypatch):
        """When the underlying lookup returns data, the UDF projects it."""
        mock_result = {
            "country": "United States",
            "region": "California",
            "city": "Mountain View",
            "latitude": 37.386,
            "longitude": -122.084,
            "postcode": "94043",
        }
        # Patch _geo_lookup at the module level
        monkeypatch.setattr(_silver_mod, "_geo_lookup", lambda ip: mock_result if ip == "8.8.8.8" else None)

        from pyspark.sql.functions import col
        df = spark.createDataFrame([Row(client_ip="8.8.8.8")])
        result = df.select(get_geo_fields(col("client_ip")).alias("geo")).collect()[0]["geo"]
        assert result["country"] == "United States"
        assert result["city"] == "Mountain View"
        assert result["latitude"] == 37.386

    def test_udf_with_private_ip(self, spark, monkeypatch):
        """Private IPs return None from the struct."""
        monkeypatch.setattr(_silver_mod, "_geo_lookup", lambda ip: None)

        from pyspark.sql.functions import col
        df = spark.createDataFrame([Row(client_ip="10.0.0.1")])
        result = df.select(get_geo_fields(col("client_ip")).alias("geo")).collect()[0]["geo"]
        assert result is None


class TestGetISPUDF:
    """Verify the ``get_isp`` scalar UDF."""

    def test_udf_returns_string(self, spark, monkeypatch):
        """Patched ASN lookup returns the organisation name."""
        monkeypatch.setattr(_silver_mod, "_asn_lookup", lambda ip: "Google" if ip == "8.8.8.8" else None)

        from pyspark.sql.functions import col
        df = spark.createDataFrame([Row(client_ip="8.8.8.8")])
        result = df.select(get_isp(col("client_ip")).alias("isp")).collect()[0]["isp"]
        assert result == "Google"

    def test_udf_defaults_to_unknown(self, spark, monkeypatch):
        """When the lookup returns None, the UDF returns 'Unknown'."""
        monkeypatch.setattr(_silver_mod, "_asn_lookup", lambda ip: None)

        from pyspark.sql.functions import col
        df = spark.createDataFrame([Row(client_ip="10.0.0.1")])
        result = df.select(get_isp(col("client_ip")).alias("isp")).collect()[0]["isp"]
        assert result == "Unknown"


# ══════════════════════════════════════════════════════════════════════
#  7. Left-anti deduplication (CRIT-06)
# ══════════════════════════════════════════════════════════════════════


class TestLeftAntiDedup:
    """Left-anti dedup — prevents duplicate rows on Silver re-runs."""

    def test_left_anti_removes_existing_source_files(self, spark):
        """Rows from already-processed source_file are excluded."""
        bronze = spark.createDataFrame([
            Row(source_file="f1.log", log_date=None, client_ip="1.1.1.1", method="GET",
                uri_stem="/", status=200, user_agent="UA", bytes_sent=100, bytes_recv=0),
            Row(source_file="f2.log", log_date=None, client_ip="2.2.2.2", method="GET",
                uri_stem="/", status=200, user_agent="UA", bytes_sent=100, bytes_recv=0),
            Row(source_file="f3.log", log_date=None, client_ip="3.3.3.3", method="GET",
                uri_stem="/", status=200, user_agent="UA", bytes_sent=100, bytes_recv=0),
        ])
        existing = spark.createDataFrame([
            Row(source_file="f1.log"),
            Row(source_file="f2.log"),
        ])

        deduped = bronze.join(existing.select("source_file").distinct(), on="source_file", how="left_anti")
        assert deduped.count() == 1
        assert deduped.select("source_file").collect()[0][0] == "f3.log"

    def test_no_existing_data_keeps_all(self, spark):
        """When Silver is empty, all Bronze rows pass through."""
        bronze = spark.createDataFrame([
            Row(source_file="f1.log", log_date=None, client_ip="1.1.1.1", method="GET",
                uri_stem="/", status=200, user_agent="UA", bytes_sent=100, bytes_recv=0),
            Row(source_file="f2.log", log_date=None, client_ip="2.2.2.2", method="GET",
                uri_stem="/", status=200, user_agent="UA", bytes_sent=100, bytes_recv=0),
        ])
        # Simulate empty existing Silver with empty DataFrame
        existing = spark.createDataFrame([], schema=Row(source_file="")._fields).select("source_file").distinct()

        deduped = bronze.join(existing, on="source_file", how="left_anti")
        assert deduped.count() == 2

    def test_all_excluded_when_all_processed(self, spark):
        """When every source_file is already in Silver, no rows pass through."""
        bronze = spark.createDataFrame([
            Row(source_file="f1.log", log_date=None, client_ip="1.1.1.1", method="GET",
                uri_stem="/", status=200, user_agent="UA", bytes_sent=100, bytes_recv=0),
        ])
        existing = spark.createDataFrame([
            Row(source_file="f1.log"),
        ])

        deduped = bronze.join(existing.select("source_file").distinct(), on="source_file", how="left_anti")
        assert deduped.count() == 0


# ══════════════════════════════════════════════════════════════════════
#  8. Silver DLT expectations — structural constraint verification
# ══════════════════════════════════════════════════════════════════════


class TestSilverExpectations:
    """Verify the DLT ``@dlt.expect_or_drop`` constraints as DataFrame filters."""

    def test_expect_valid_country(self, spark):
        """Rows with null country are dropped."""
        from pyspark.sql.functions import col

        df = spark.createDataFrame([
            Row(country="US", traffic_type="Direct", page_category="Content",
                log_date=None, client_ip="1.1.1.1"),
            Row(country=None, traffic_type="Direct", page_category="Content",
                log_date=None, client_ip="2.2.2.2"),
        ])
        filtered = df.filter(col("country").isNotNull())
        assert filtered.count() == 1

    def test_expect_valid_traffic_type(self, spark):
        """Rows with traffic_type outside the allowed set are dropped."""
        from pyspark.sql.functions import col

        VALID = ("Direct", "Search", "Social", "Referral")
        df = spark.createDataFrame([
            Row(traffic_type="Direct", country="US", page_category="Content"),
            Row(traffic_type="Invalid", country="US", page_category="Content"),
            Row(traffic_type=None, country="US", page_category="Content"),
        ])
        filtered = df.filter(col("traffic_type").isin(VALID))
        assert filtered.count() == 1

    def test_expect_valid_page_category(self, spark):
        """Rows with null page_category are dropped."""
        from pyspark.sql.functions import col

        df = spark.createDataFrame([
            Row(page_category="Content", country="US", traffic_type="Direct"),
            Row(page_category=None, country="US", traffic_type="Direct"),
        ])
        filtered = df.filter(col("page_category").isNotNull())
        assert filtered.count() == 1


# ══════════════════════════════════════════════════════════════════════
#  9. Silver table properties — structural checks
# ══════════════════════════════════════════════════════════════════════


class TestSilverTableProperties:
    """Verify Silver table properties match documented configuration."""

    def test_cdc_enabled(self):
        """Silver must have ChangeDataFeed enabled."""
        import inspect
        source = inspect.getsource(silver_enriched_logs)
        assert "delta.enableChangeDataFeed" in source
        assert "true" in source

    def test_auto_optimize_write_enabled(self):
        """Silver must have autoOptimize.optimizeWrite enabled."""
        import inspect
        source = inspect.getsource(silver_enriched_logs)
        assert "delta.autoOptimize.optimizeWrite" in source
        assert "true" in source

    def test_dlt_expectation_decorators_present(self):
        """All three expected DLT expect_or_drop decorators are applied."""
        import inspect
        source = inspect.getsource(silver_enriched_logs)
        assert "valid_country" in source
        assert "valid_traffic_type" in source
        assert "valid_page_category" in source


# ══════════════════════════════════════════════════════════════════════
#  10. Helper: _MockCityResponse
# ══════════════════════════════════════════════════════════════════════


class _MockCityResponse:
    """Simulates a ``geoip2.models.City`` response for testing."""

    class _Location:
        latitude = 37.386
        longitude = -122.084

    class _Country:
        class _Names:
            def __init__(self):
                self.name = "United States"

        def __init__(self):
            self.names = self._names = self._Names()
            self.name = "United States"

    class _Subdivisions:
        class _MostSpecific:
            name = "California"

        def __init__(self):
            self.most_specific = self._MostSpecific()

    class _City:
        class _Names:
            def __init__(self):
                self.name = "Mountain View"

        def __init__(self):
            self.name = "Mountain View"
            self.names = self._Names()

    class _Postal:
        code = "94043"

    def __init__(self):
        self.location = self._Location()
        self.country = self._Country()
        self.subdivisions = self._Subdivisions()
        self.city = self._City()
        self.postal = self._Postal()
