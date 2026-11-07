"""Tests for previously untested helpers (gap coverage)."""

import os
import sys

_TEST_DIR = os.path.dirname(os.path.abspath(__file__))
_SCRIPTS_DIR = os.path.join(_TEST_DIR, "..", "pipeline", "scripts")
if _SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, _SCRIPTS_DIR)

from emit_quality_facet import summarize_run_results  # noqa: E402


def test_summarize_run_results_all_pass():
    results = {"results": [{"status": "pass"}, {"status": "pass"}]}
    summary = summarize_run_results(results)
    assert summary["totalTests"] == 2
    assert summary["passed"] == 2
    assert summary["failed"] == 0


def test_summarize_run_results_partial():
    results = {"results": [{"status": "pass"}, {"status": "fail", "unique_id": "x"}]}
    summary = summarize_run_results(results)
    assert summary["totalTests"] == 2
    assert summary["passed"] == 1
    assert summary["failed"] == 1


def test_summarize_run_results_empty():
    summary = summarize_run_results({})
    assert summary["totalTests"] == 0
    assert summary["passed"] == 0
