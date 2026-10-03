"""Unit tests for the ``w3c_dbt_marts_azure`` DAG's task builder.

``_build_dbt_task`` is a pure function — serverless Databricks requires the
``tasks`` array format even for single-notebook submissions. These tests pin
the payload shape without needing a Databricks connection.

Requires Airflow to be importable (the DAG module imports airflow + providers);
skipped otherwise, same contract as test_dag_integrity.py.
"""

from __future__ import annotations

import os
import sys

import pytest

pytest.importorskip("airflow.models")

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
for _path in (
    os.path.join(_PROJECT_ROOT, "pipeline", "dags", "w3c"),
    os.path.join(_PROJECT_ROOT, "pipeline", "plugins"),
    "/opt/airflow/dags/w3c",
    "/opt/airflow/plugins",
):
    if os.path.isdir(_path) and _path not in sys.path:
        sys.path.insert(0, _path)


@pytest.mark.dag_integrity
class TestBuildDbtTask:
    def test_single_task_wrapper(self):
        from dbt_marts_azure import _build_dbt_task

        tasks = _build_dbt_task("dbt_run", "dbt_run.py")
        assert isinstance(tasks, list) and len(tasks) == 1

    @pytest.mark.parametrize(
        "notebook_name",
        ["dbt_freshness.py", "dbt_run.py", "dbt_test.py", "dbt_docs.py"],
    )
    def test_notebook_path_shape(self, notebook_name):
        from dbt_marts_azure import _REPO_ROOT, _build_dbt_task

        (task,) = _build_dbt_task("dbt_run", notebook_name)
        assert task["task_key"] == "dbt_run"
        expected = f"{_REPO_ROOT}/pipeline/spark/databricks/{notebook_name}"
        assert task["notebook_task"]["notebook_path"] == expected
