"""Unit tests for medallion StructType schemas."""

import os
import sys

_TEST_DIR = os.path.dirname(os.path.abspath(__file__))
_JOBS_DIR = os.path.join(_TEST_DIR, "..", "pipeline", "spark", "jobs")
if _JOBS_DIR not in sys.path:
    sys.path.insert(0, _JOBS_DIR)

from utils.schemas import bronze_schema, gold_schema, silver_schema  # noqa: E402


def _names(schema):
    return [f.name for f in schema.fields]


def test_bronze_has_19_fields():
    assert len(_names(bronze_schema)) == 19


def test_silver_extends_bronze_with_enrichment_fields():
    bronze = _names(bronze_schema)
    silver = _names(silver_schema)
    assert silver[: len(bronze)] == bronze
    for extra in (
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
        "page_category",
        "referrer_domain",
        "traffic_type",
        "is_crawler",
        "size_band",
    ):
        assert extra in silver
    assert len(silver) == 36


def test_gold_has_25_export_columns():
    gold = _names(gold_schema)
    assert len(gold) == 25
    assert "log_date" in gold
    assert "size_band" in gold
    # Gold is the warehouse export subset, not the full silver row.
    assert gold != _names(silver_schema)
