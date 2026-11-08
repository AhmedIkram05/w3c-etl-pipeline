"""Contract tests for the fact_webrequest incremental lookback window.

The incremental filter used to be ``log_date >= (SELECT MAX(log_date) FROM
this)``, which silently dropped any source file re-uploaded for a day older
than the watermark. The window is now the watermark minus
``var('fact_webrequest_lookback_days')``, so a recent back-dated re-upload is
re-merged. These tests fail if the subtraction is removed or the var is
misconfigured, either of which would reintroduce the silent drop without
breaking compilation.
"""

from __future__ import annotations

import re
from pathlib import Path

import yaml

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_CANDIDATES = [
    _PROJECT_ROOT / "pipeline" / "dbt" / "w3c" / "dbt_project.yml",
    _PROJECT_ROOT / "dbt" / "w3c" / "dbt_project.yml",  # Docker mount layout
]
_MODEL_CANDIDATES = [
    _PROJECT_ROOT / "pipeline" / "dbt" / "w3c" / "models" / "staging" / "fact_webrequest.sql",
    _PROJECT_ROOT / "dbt" / "w3c" / "models" / "staging" / "fact_webrequest.sql",
]


def _first_existing(candidates: list[Path]) -> Path:
    return next((p for p in candidates if p.exists()), candidates[0])


def _model_sql() -> str:
    return _first_existing(_MODEL_CANDIDATES).read_text()


def test_lookback_var_is_a_positive_int() -> None:
    project = yaml.safe_load(_first_existing(_CANDIDATES).read_text())
    lookback = project["vars"]["fact_webrequest_lookback_days"]
    assert isinstance(lookback, int), f"lookback must be an int, got {type(lookback).__name__}"
    assert lookback > 0, "a zero/negative lookback disables back-dated re-uploads"


def test_incremental_filter_applies_the_lookback() -> None:
    sql = _model_sql()
    assert "{{ var('fact_webrequest_lookback_days') }}" in sql
    assert re.search(r"MAX\(log_date\) FROM \{\{ this \}\}\)\s*-\s*\{\{ var\('fact_webrequest_lookback_days'\) \}\}", sql), (
        "the watermark must be shifted back by the lookback var"
    )


def test_incremental_filter_is_not_a_bare_watermark() -> None:
    """Regression: the bare MAX(log_date) filter dropped back-dated re-uploads."""
    sql = _model_sql()
    assert not re.search(r">=\s*\(SELECT MAX\(log_date\) FROM \{\{ this \}\}\)\s*\n", sql), (
        "bare watermark filter is back; re-uploads older than MAX(log_date) will be dropped"
    )
