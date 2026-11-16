"""Tests for the batch Silver enrichment's distinct-key lookup path.

The batch path enriches each distinct ``client_ip`` / ``user_agent`` once and
joins the small lookup frames back, instead of invoking the Python UDFs once
per row. These tests pin the two properties that make that safe: the lookup
frames are keyed 1:1, and the joins neither fan out nor drop the "Unknown"
enrichment for NULL/placeholder keys.
"""

import os
import sys

import pytest

pytest.importorskip("pyspark")

_TEST_DIR = os.path.dirname(os.path.abspath(__file__))
_JOBS_DIR = os.path.join(_TEST_DIR, "..", "pipeline", "spark", "jobs")
if _JOBS_DIR not in sys.path:
    sys.path.insert(0, _JOBS_DIR)

# Workers must use the same interpreter as the driver for the UDF deps.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)

import silver_enrichment  # noqa: E402
from utils.geoip import asn_lookup, city_lookup  # noqa: E402
from utils.ua_parser import parse_ua_fields  # noqa: E402

# The test environment has no Delta jars; map the production format to
# Parquet so the persistence paths run for real.
from pyspark.sql.readwriter import DataFrameReader, DataFrameWriter  # noqa: E402

_orig_w = DataFrameWriter.format


def _w(self, source):
    return _orig_w(self, "parquet" if source == "delta" else source)


DataFrameWriter.format = _w
_orig_r = DataFrameReader.format


def _r(self, source):
    return _orig_r(self, "parquet" if source == "delta" else source)


DataFrameReader.format = _r


def _df(spark):
    return spark.createDataFrame(
        [
            ("10.0.0.1", "Mozilla/4.0 (compatible; MSIE 7.0)", "/a", "-", 100, 50),
            ("10.0.0.1", "Mozilla/4.0 (compatible; MSIE 7.0)", "/b", "-", 100, 50),
            ("192.168.1.2", "Mozilla/5.0", "/c", "http://google.com", 0, 0),
            ("-", "-", "/d", "-", 0, 0),
            # NULL keys are legitimate in the wild; eqNullSafe must still
            # attach the UDF's "Unknown" struct rather than a null one.
            (None, None, "/e", "-", 0, 0),
        ],
        "client_ip string, user_agent string, uri_stem string, referrer string, bytes_sent long, bytes_recv long",
    )


def test_geo_lookup_frame_is_one_row_per_distinct_ip(spark):
    df = _df(spark)
    lookup = silver_enrichment.geo_lookup_frame(df, city_lookup(None), asn_lookup(None))
    assert lookup.count() == df.select("client_ip").distinct().count() == 4
    assert {"client_ip", "geo", "isp"} <= set(lookup.columns)


def test_ua_lookup_frame_is_one_row_per_distinct_ua(spark):
    df = _df(spark)
    lookup = silver_enrichment.ua_lookup_frame(df, parse_ua_fields)
    assert lookup.count() == df.select("user_agent").distinct().count() == 4
    assert {"user_agent", "ua"} <= set(lookup.columns)


def test_enrich_distinct_preserves_row_count_and_unknowns(spark):
    df = _df(spark)
    out = silver_enrichment.enrich_distinct(df, city_lookup(None), asn_lookup(None), parse_ua_fields)

    # A 1:1 lookup join must not change the fact grain.
    assert out.count() == df.count() == 5
    assert {
        "country",
        "region",
        "city",
        "latitude",
        "longitude",
        "postcode",
        "isp",
        "agent_type",
        "browser_name",
        "browser_version",
        "operating_system",
        "device_type",
    } <= set(out.columns)

    # Private/placeholder IPs are absent from GeoLite2; every row must still
    # carry "Unknown" rather than a null struct.
    countries = {r.country for r in out.select("country").distinct().collect()}
    assert countries == {"Unknown"}

    dashes = out.filter(out.user_agent == "-").select("agent_type").collect()
    assert len(dashes) == 1 and dashes[0].agent_type == "Unknown"


def test_null_keys_still_receive_unknown_enrichment(spark):
    """eqNullSafe is load-bearing: NULL keys must match the lookup row."""
    df = _df(spark)
    out = silver_enrichment.enrich_distinct(df, city_lookup(None), asn_lookup(None), parse_ua_fields)
    null_rows = out.filter(out.client_ip.isNull()).select("country", "agent_type").collect()
    assert len(null_rows) == 1
    assert null_rows[0].country == "Unknown"
    assert null_rows[0].agent_type == "Unknown"


def test_crawler_ips_are_persisted_and_reused(spark, tmp_path):
    """First run seeds from Bronze; later runs union only new rows."""
    import shutil

    delta = str(tmp_path)
    bronze = spark.createDataFrame(
        [("1.1.1.1", "/robots.txt"), ("2.2.2.2", "/index.html")],
        "client_ip string, uri_stem string",
    )
    bronze.write.format("delta").mode("overwrite").save(os.path.join(delta, "bronze"))

    first = silver_enrichment.discover_crawler_ips(spark, delta)
    assert first == {"1.1.1.1"}

    new_data = spark.createDataFrame(
        [("3.3.3.3", "/robots.txt"), ("4.4.4.4", "/about")],
        "client_ip string, uri_stem string",
    )
    second = silver_enrichment.discover_crawler_ips(spark, delta, new_data=new_data)
    assert second == {"1.1.1.1", "3.3.3.3"}

    # Prove the persisted set is reused without touching Bronze.
    shutil.rmtree(os.path.join(delta, "bronze"))
    third = silver_enrichment.discover_crawler_ips(spark, delta)
    assert third == {"1.1.1.1", "3.3.3.3"}


def test_safe_parse_handles_non_string_values():
    """Pandas passes NULL as NaN (float); _safe_parse must not call .strip()."""
    from utils.ua_parser import _safe_parse

    for value in (None, float("nan"), "", "-", "Unknown"):
        result = _safe_parse(value)
        assert result["agent_type"] == "Unknown"
        assert result["browser_name"] == "Unknown"
        assert result["device_type"] == "Unknown"


def test_duplicate_keys_do_not_fan_out(spark):
    df = _df(spark).filter("client_ip = '10.0.0.1'")
    out = silver_enrichment.enrich_distinct(df, city_lookup(None), asn_lookup(None), parse_ua_fields)
    assert out.count() == 2
