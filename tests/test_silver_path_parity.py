"""
Cross-path parity for the Silver enrichment builders.

The same source row must yield the SAME types and the SAME classification
vocabulary no matter which Silver path produced it:

* local Spark  — ``spark/jobs/silver_enrichment.py`` + ``utils/transformations``
* Databricks   — ``spark/databricks/02_silver_enrichment.py``
* DLT          — ``spark/databricks/dlt_silver.py``

``dlt_silver``'s crawler flag is user-agent based while the other two are
IP based, so only the ``is_crawler`` *type* (BOOLEAN) is asserted — the
value legitimately differs by algorithm.
"""

import importlib
import os
import sys
from unittest.mock import MagicMock

import pytest

pytest.importorskip("pyspark")
from pyspark.sql.types import BooleanType, FloatType

# ── dlt is only importable with a stub outside Databricks ─────────────
_dlt_stub = MagicMock()
_dlt_stub.table.return_value = lambda f: f
_dlt_stub.streaming_table.return_value = lambda f: f
_dlt_stub.expect_or_drop.return_value = lambda f: f
sys.modules.setdefault("dlt", _dlt_stub)

_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
for _path in (
    os.path.join(_ROOT, "pipeline", "spark", "databricks"),
    os.path.join(_ROOT, "pipeline", "spark"),
):
    if os.path.isdir(_path) and _path not in sys.path:
        sys.path.insert(0, _path)

import dlt_silver as dlt  # noqa: E402
from utils import transformations as local  # noqa: E402
from utils.geoip import city_lookup  # noqa: E402

silver02 = importlib.import_module("02_silver_enrichment")

# Same input row fed to every path.
REFERRERS = [
    "http://www.google.com/search?q=darwin",
    "https://facebook.com/post/1",
    "http://www.darwinsbeagleplants.org/Darwin/Plant.aspx",
    "http://example.org/page",
    "www.example.com/path",
    "https://t.co/abc123",
    "http://abcwww.com/page",
    "http://notgoogle.com/page",
    "-",
    "",
    "Unknown",
]


def test_referrer_domain_identical_across_paths():
    """All three paths extract the same referrer domain (leading www. only)."""
    for ref in REFERRERS:
        domains = {
            silver02._referrer_domain(ref),
            dlt._extract_domain(ref),
            local._extract_domain(ref),
        }
        assert len(domains) == 1, f"{ref!r} -> {domains}"


def test_traffic_type_vocabulary_identical():
    """02 and DLT emit the same traffic vocabulary for the same referrer."""
    dlt_classify = dlt.get_traffic_type.func  # the UDF's underlying Python function
    for ref in REFERRERS:
        assert silver02._traffic_type(ref) == dlt_classify(dlt._extract_domain(ref)), ref


def test_traffic_classification_lists_identical():
    """The canonical pattern lists are shared verbatim across paths."""
    assert local._SEARCH_ENGINES == dlt._SEARCH_ENGINES == silver02._SEARCH_ENGINES
    assert local._SOCIAL_DOMAINS == dlt._SOCIAL_DOMAINS == silver02._SOCIAL_DOMAINS
    assert local._INTERNAL_DOMAINS == dlt._INTERNAL_DOMAINS == silver02._INTERNAL_DOMAINS


def test_is_crawler_is_boolean_everywhere():
    """Every path declares is_crawler as BOOLEAN (canonical contract)."""
    assert dlt.get_is_crawler.returnType == BooleanType()
    assert silver02.make_crawler_udf(set()).returnType == BooleanType()
    assert local.make_crawler_udf(set()).returnType == BooleanType()


def test_coordinates_are_float_everywhere():
    """Every path declares latitude/longitude as FLOAT (canonical contract)."""
    dlt_geo = {f.name: f.dataType for f in dlt.get_geo_fields.returnType.fields}
    assert dlt_geo["latitude"] == FloatType()
    assert dlt_geo["longitude"] == FloatType()

    o2_geo = {f.name: f.dataType for f in silver02._GEO_STRUCT_SCHEMA.fields}
    assert o2_geo["latitude"] == FloatType()
    assert o2_geo["longitude"] == FloatType()

    local_geo = {f.name: f.dataType for f in city_lookup(None).returnType.fields}
    assert local_geo["latitude"] == FloatType()
    assert local_geo["longitude"] == FloatType()


# ── _geo_lookup contract parity (dlt vs 02) ───────────────────────────
# The two implementations cannot share a reader (maxminddb vs geoip2, and the
# DLT file must stay self-contained), so the contract is what must match:
# a hit is a dict, a miss is None. The per-path extra field differs with the
# target schema (DLT silver has postcode, UC silver has isp).

_GEO_SHARED_KEYS = {"country", "region", "city", "latitude", "longitude"}


def test_geo_lookup_return_annotations_match():
    """Both twins declare the same return contract (dict | None)."""
    from typing import get_type_hints

    assert get_type_hints(dlt._geo_lookup)["return"] == get_type_hints(silver02._geo_lookup)["return"]


def test_geo_lookup_miss_returns_none_for_both(monkeypatch):
    """A miss returns None on both paths, never an "Unknown" dict."""
    monkeypatch.setattr(silver02, "_geoip_reader", None)
    assert dlt._geo_lookup("10.0.0.1") is None
    assert silver02._geo_lookup("10.0.0.1") is None


def test_geo_lookup_hit_same_shape_for_both(monkeypatch):
    """A hit is a dict on both paths, sharing the canonical geo fields."""
    # dlt reads maxminddb, which returns a raw nested dict (postcode, no isp).
    dlt_reader = MagicMock()
    dlt_reader.get.return_value = {
        "country": {"names": {"en": "United States"}},
        "subdivisions": [{"names": {"en": "California"}}],
        "city": {"names": {"en": "Mountain View"}},
        "location": {"latitude": 37.386, "longitude": -122.084},
        "postal": {"code": "94043"},
    }
    monkeypatch.setattr(dlt, "_geo_reader", dlt_reader)
    monkeypatch.setattr(dlt, "_geo_init_attempted", True)

    # 02 reads geoip2, which returns an object (isp, no postcode).
    response = MagicMock()
    response.country.name = "United States"
    response.subdivisions.most_specific.name = "California"
    response.city.name = "Mountain View"
    response.location.latitude = 37.386
    response.location.longitude = -122.084
    response.traits.isp = "Google"
    reader = MagicMock()
    reader.city.return_value = response
    monkeypatch.setattr(silver02, "_geoip_reader", reader)

    dlt_hit = dlt._geo_lookup("8.8.8.8")
    o2_hit = silver02._geo_lookup("8.8.8.8")

    assert isinstance(dlt_hit, dict) and isinstance(o2_hit, dict)
    assert _GEO_SHARED_KEYS <= dlt_hit.keys()
    assert _GEO_SHARED_KEYS <= o2_hit.keys()
    # Documented per-path extras, driven by each target schema.
    assert set(dlt_hit) - _GEO_SHARED_KEYS == {"postcode"}
    assert set(o2_hit) - _GEO_SHARED_KEYS == {"isp"}
