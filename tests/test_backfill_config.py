"""Tests for the BackfillConfig dataclass."""

import os
import sys

import pytest

_TEST_DIR = os.path.dirname(os.path.abspath(__file__))
_JOBS_DIR = os.path.join(_TEST_DIR, "..", "pipeline", "spark", "jobs")
if _JOBS_DIR not in sys.path:
    sys.path.insert(0, _JOBS_DIR)

import silver_backfill  # noqa: E402
from silver_backfill import BackfillConfig  # noqa: E402


def test_defaults():
    cfg = BackfillConfig("2009-10-24", "2009-10-26")
    assert cfg.delta_dir == "/opt/spark/delta"
    assert cfg.salt_buckets == 16
    cfg.validate()


def test_invalid_salt_rejected():
    cfg = BackfillConfig("2009-10-24", "2009-10-26", salt_buckets=0)
    with pytest.raises(ValueError, match="salt_buckets"):
        cfg.validate()


def test_run_delegates_to_run_with_config(monkeypatch):
    calls = {}

    def fake_run(spark, cfg):
        calls["cfg"] = cfg
        return {"ok": True}

    monkeypatch.setattr(silver_backfill, "run_with_config", fake_run)
    result = silver_backfill.run(None, "2009-10-24", "2009-10-26", salt_buckets=8)
    assert result == {"ok": True}
    assert calls["cfg"].end_date == "2009-10-26"
    assert calls["cfg"].salt_buckets == 8
