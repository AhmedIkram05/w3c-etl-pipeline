"""Unit tests for medallion StructType schemas."""

import os
import sys

_TEST_DIR = os.path.dirname(os.path.abspath(__file__))
_JOBS_DIR = os.path.join(_TEST_DIR, "..", "pipeline", "spark", "jobs")
if _JOBS_DIR not in sys.path:
    sys.path.insert(0, _JOBS_DIR)

import export_warehouse  # noqa: E402
from utils.schemas import bronze_schema, gold_schema  # noqa: E402


def _names(schema):
    return [f.name for f in schema.fields]


def _ddl_columns(ddl):
    body = ddl.split("(", 1)[1].rsplit(")", 1)[0]
    return [line.split()[0] for line in body.strip().splitlines() if line.strip()]


def test_bronze_has_19_fields():
    assert len(_names(bronze_schema)) == 19


def test_gold_has_25_export_columns():
    gold = _names(gold_schema)
    assert len(gold) == 25
    assert "log_date" in gold
    assert "size_band" in gold
    # Gold is the warehouse export subset, not the full bronze row.
    assert gold != _names(bronze_schema)


def test_gold_matches_raw_enriched_ddl():
    """The export select and the warehouse DDL must list the same columns.

    export_warehouse selects by ``gold_schema`` while RAW_ENRICHED_DDL is
    hand-written SQL. If they disagree the JDBC write fails with
    COLUMN_NOT_DEFINED_IN_TABLE at runtime, so pin the names and their order.
    """
    assert _ddl_columns(export_warehouse.RAW_ENRICHED_DDL) == _names(gold_schema)


def test_gold_columns_exist_in_silver():
    """Every exported column must be produced somewhere upstream."""
    silver_only = {
        "postcode",
        "page_category",
        "referrer_domain",
        "traffic_type",
        "is_crawler",
        "size_band",
    }
    bronze = set(_names(bronze_schema))
    for name in _names(gold_schema):
        assert name in bronze or name in silver_only, f"{name} is not a known silver column"
