"""Unit tests for the OpenLineage data-quality facet emitter.

Covers the pure aggregation logic in airflow/scripts/emit_quality_facet.py.
The module is stdlib-only at import time (openlineage is imported lazily
inside ``emit``), so these run anywhere pytest runs — no Airflow needed.
"""

import importlib.util
from pathlib import Path

_SCRIPT = (
    Path(__file__).resolve().parents[1] / "airflow" / "scripts" / "emit_quality_facet.py"
)
_spec = importlib.util.spec_from_file_location("emit_quality_facet", _SCRIPT)
if _spec is None or _spec.loader is None:
    raise ImportError(f"Cannot load emitter module from {_SCRIPT}")
emit_quality = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(emit_quality)


def _result(unique_id: str, status: str, message: str | None = None) -> dict:
    return {"unique_id": unique_id, "status": status, "message": message}


def test_all_passing_tests_produce_clean_summary():
    results = {
        "results": [
            _result("model.w3c.a", "pass"),
            _result("test.w3c.b.1", "pass"),
            _result("test.w3c.c.1", "pass"),
        ]
    }
    summary = emit_quality.summarize_run_results(results)
    assert summary == {
        "totalTests": 3,
        "passed": 3,
        "failed": 0,
        "skipped": 0,
        "warnings": 0,
        "failures": [],
    }


def test_failures_are_captured_with_truncated_messages():
    long_message = "x" * 1000
    results = {
        "results": [
            _result("test.w3c.not_null.1", "fail", long_message),
            _result("test.w3c.unique.1", "error", "boom"),
            _result("test.w3c.ok.1", "pass"),
        ]
    }
    summary = emit_quality.summarize_run_results(results)
    assert summary["totalTests"] == 3
    assert summary["passed"] == 1
    assert summary["failed"] == 2  # fail + error both count as failed
    assert len(summary["failures"]) == 2
    first = summary["failures"][0]
    assert first["unique_id"] == "test.w3c.not_null.1"
    assert len(first["message"]) == emit_quality.MESSAGE_TRUNCATE  # truncated


def test_skips_warnings_and_unknown_statuses_are_bucketed_correctly():
    results = {
        "results": [
            _result("test.w3c.s.1", "skip"),
            _result("test.w3c.w.1", "warn"),
            _result("test.w3c.u.1", "some_future_status"),
        ]
    }
    summary = emit_quality.summarize_run_results(results)
    assert summary["totalTests"] == 3  # unknown status still counted in total
    assert summary["skipped"] == 1
    assert summary["warnings"] == 1
    assert summary["passed"] == 0
    assert summary["failed"] == 0
    assert summary["failures"] == []


def test_failure_list_is_capped_at_max_failures():
    rows = [_result(f"test.w3c.f.{i}", "fail") for i in range(50)]
    summary = emit_quality.summarize_run_results({"results": rows})
    assert summary["totalTests"] == 50
    assert summary["failed"] == 50
    assert len(summary["failures"]) == emit_quality.MAX_FAILURES


def test_empty_results_give_zeroed_payload():
    summary = emit_quality.summarize_run_results({"results": []})
    assert summary["totalTests"] == 0
    assert summary["failures"] == []
