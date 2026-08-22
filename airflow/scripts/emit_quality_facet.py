#!/usr/bin/env python3
"""
Emit a custom OpenLineage run facet summarising dbt test outcomes.

After `dbt test`, reads dbt's target/run_results.json and emits a COMPLETE
RunEvent carrying a ``w3cDataQuality`` run facet for the same job identity as
the `dbt-ol run` events — so data-quality results are attached, in Marquez,
to the lineage node that produced the data rather than living only in Airflow logs.

Identity matching relies on two env vars shared with the DAG tasks
(see airflow/docker-compose.yaml):
  - OPENLINEAGE_NAMESPACE   (default: w3c_etl_local)
  - OPENLINEAGE_DBT_JOB_NAME (default: w3c_dbt_marts_dbt_run)

Failure policy (deliberate): a missing results file or a transport error is a
WARN + exit 0 — lineage emission must never break the pipeline. A *malformed*
results file exits 1, because that indicates real dbt breakage upstream.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

PRODUCER = "https://github.com/AhmedIkram05/w3c-etl-pipeline"
DEFAULT_RESULTS = "/opt/airflow/dbt/w3c/target/run_results.json"
MAX_FAILURES = 20  # ponytail: cap facet size; full detail stays in run_results.json
MESSAGE_TRUNCATE = 500

# dbt result statuses (run_results.json -> results[].status)
_COUNTED_STATUSES = ("pass", "fail", "skip", "error", "warn")


def summarize_run_results(results: dict) -> dict:
    """Aggregate dbt run_results.json into a facet payload. Pure function."""
    rows = results.get("results", [])
    counts = {status: 0 for status in _COUNTED_STATUSES}
    failures: list[dict] = []
    for row in rows:
        status = row.get("status")
        if status not in counts:
            continue  # unknown future status — counted in totalTests, not bucketed
        counts[status] += 1
        if status in ("fail", "error") and len(failures) < MAX_FAILURES:
            failures.append({
                "unique_id": row.get("unique_id"),
                "status": status,
                "message": (row.get("message") or "")[:MESSAGE_TRUNCATE],
            })
    return {
        "totalTests": len(rows),
        "passed": counts["pass"],
        "failed": counts["fail"] + counts["error"],
        "skipped": counts["skip"],
        "warnings": counts["warn"],
        "failures": failures,
    }


def emit(summary: dict, namespace: str, job_name: str) -> None:
    """Build and send the RunEvent. openlineage imported lazily so the pure
    functions above stay testable without the dependency installed."""
    # pylint: disable=import-outside-toplevel
    from attrs import define
    from openlineage.client.client import OpenLineageClient, OpenLineageClientOptions
    from openlineage.client.facet import BaseFacet
    from openlineage.client.run import Job, Run, RunEvent, RunState
    from openlineage.client.uuid import generate_new_uuid

    @define
    class W3cDataQualityFacet(BaseFacet):
        total_tests: int
        passed: int
        failed: int
        skipped: int
        warnings: int
        failures: list

    event = RunEvent(
        eventType=RunState.COMPLETE,
        eventTime=datetime.now(timezone.utc).isoformat(),
        run=Run(
            runId=str(generate_new_uuid()),
            facets={"w3cDataQuality": W3cDataQualityFacet(**summary)},
        ),
        job=Job(namespace=namespace, name=job_name),
        producer=PRODUCER,
    )
    client = OpenLineageClient(
        url=os.environ.get("OPENLINEAGE_URL", "http://localhost:5000"),
        options=OpenLineageClientOptions(),
    )
    client.emit(event)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Emit dbt test outcomes as an OpenLineage run facet.")
    parser.add_argument("--results", default=os.environ.get("DBT_RUN_RESULTS", DEFAULT_RESULTS))
    parser.add_argument("--namespace", default=os.environ.get("OPENLINEAGE_NAMESPACE", "w3c_etl_local"))
    parser.add_argument("--job-name", default=os.environ.get("OPENLINEAGE_DBT_JOB_NAME", "w3c_dbt_marts_dbt_run"))
    args = parser.parse_args(argv)

    path = Path(args.results)
    if not path.exists():
        print(f"WARN: {path} not found — no quality facet to emit")
        return 0
    try:
        summary = summarize_run_results(json.loads(path.read_text()))
    except (json.JSONDecodeError, OSError) as exc:
        print(f"ERROR: could not parse {path}: {exc}")
        return 1

    try:
        emit(summary, args.namespace, args.job_name)
    except Exception as exc:  # noqa: BLE001 — best-effort by design
        print(f"WARN: failed to emit lineage event (pipeline unaffected): {exc}")
        return 0

    print(
        f"Emitted w3cDataQuality facet -> {args.namespace}/{args.job_name} "
        f"({summary['passed']}/{summary['totalTests']} passed, {summary['failed']} failed)"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
