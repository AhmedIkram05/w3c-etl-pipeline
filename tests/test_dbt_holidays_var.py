"""Structural tests for the uk_holidays dbt var backing dim_date.holiday_flag.

dim_date.sql iterates ``var('uk_holidays')`` to flag UK bank holidays. The
var must stay a sorted, unique list of weekday ISO dates spanning the
warehouse's history (2009-) plus the computed future horizon (through 2030);
a missing date silently reports holiday_flag 'No'.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest
import yaml

_PROJECT_ROOT = Path(__file__).resolve().parent.parent


def _load_holiday_var() -> list[str]:
    candidates = [
        _PROJECT_ROOT / "pipeline" / "dbt" / "w3c" / "dbt_project.yml",
        _PROJECT_ROOT / "dbt" / "w3c" / "dbt_project.yml",  # Docker mount layout
    ]
    project_file = next((p for p in candidates if p.exists()), candidates[0])
    with open(project_file) as handle:
        project = yaml.safe_load(handle)
    return project["vars"]["uk_holidays"]


def test_holiday_var_is_sorted_unique_iso_weekdays():
    holidays = _load_holiday_var()
    assert holidays, "uk_holidays var must not be empty"
    parsed = [date.fromisoformat(d) for d in holidays]
    assert len(parsed) == len(set(parsed)), "duplicate holiday dates"
    assert parsed == sorted(parsed), "uk_holidays must stay sorted"
    for d in parsed:
        assert d.weekday() < 5, f"{d.isoformat()} is not a weekday"


@pytest.mark.parametrize(
    "expected",
    [
        # Known moved/one-off specials and computed Easters.
        "2012-06-04",  # spring bank holiday moved for the Diamond Jubilee
        "2012-06-05",  # Diamond Jubilee one-off
        "2020-05-08",  # early May bank holiday moved to VE Day 75
        "2022-06-03",  # Platinum Jubilee one-off
        "2022-09-19",  # Queen Elizabeth II state funeral one-off
        "2023-05-08",  # Coronation one-off
        "2025-04-21",  # Easter Monday 2025
        "2026-12-25",  # Christmas 2026 falls on a Friday — no substitution
        "2026-12-28",  # Boxing Day 2026 Saturday substitute
    ],
)
def test_holiday_var_known_dates(expected):
    assert expected in _load_holiday_var()


def test_holiday_var_coverage_2009_to_2030():
    parsed = [date.fromisoformat(d) for d in _load_holiday_var()]
    assert min(parsed) == date(2009, 1, 1)
    assert max(parsed) == date(2030, 12, 26)
    for year in range(2009, 2031):
        assert any(d.year == year for d in parsed), f"no holidays for {year}"
