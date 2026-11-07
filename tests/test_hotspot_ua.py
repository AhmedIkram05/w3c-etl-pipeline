"""Tests for the decomposed Azure UA helper (behavior preserved)."""

import os
import sys
from types import SimpleNamespace

_TEST_DIR = os.path.dirname(os.path.abspath(__file__))
_DAGS_DIR = os.path.join(_TEST_DIR, "..", "pipeline", "dags")
if _DAGS_DIR not in sys.path:
    sys.path.insert(0, _DAGS_DIR)

from dags.w3c.spark_ingestion_azure import _build_useragent_tuple, _parse_user_agent_rows  # noqa: E402


def _fake_parse(s):
    return SimpleNamespace(
        is_bot=False,
        is_mobile=False,
        is_tablet=False,
        is_pc=True,
        browser=SimpleNamespace(family="Chrome", version_string="120"),
        os=SimpleNamespace(family="Windows"),
    )


def test_build_useragent_tuple():
    row = _build_useragent_tuple("Mozilla/5.0", _fake_parse)
    assert row[2] == "Browser"
    assert row[3] == "Chrome"
    assert row[4] == "120"
    assert row[5] == "Windows"
    assert row[6] == "Desktop"
    assert len(row[0]) == 32  # md5 hex


def test_parse_rows_dedupes():
    ua = "Mozilla/5.0 (Windows)"
    rows = _parse_user_agent_rows([(ua,), (ua,)])
    assert len(rows) == 1
    assert rows[0][1] == ua
