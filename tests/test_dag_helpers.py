"""Coverage for the DAG helper functions that had no tests."""

from types import SimpleNamespace

import pytest
from airflow.exceptions import AirflowSkipException
from dags.w3c import dbt_marts_azure, spark_ingestion, spark_ingestion_azure


class TestBuildDbtTask:
    def test_builds_single_notebook_task(self):
        (task,) = dbt_marts_azure._build_dbt_task("dbt_run", "dbt_run.py")
        assert task["task_key"] == "dbt_run"
        assert task["notebook_task"]["notebook_path"] == (
            f"{dbt_marts_azure._REPO_ROOT}/pipeline/spark/databricks/dbt_run.py"
        )


class TestExportDimensionsWithOutlet:
    def test_emits_outlet_after_build(self, monkeypatch):
        calls = []
        monkeypatch.setattr(spark_ingestion, "_export_dimensions", lambda: calls.append("build"))
        outlet = SimpleNamespace(emit=lambda: calls.append("emit"))
        spark_ingestion._export_dimensions_with_outlet(outlet_events={spark_ingestion.WAREHOUSE_LOADED: outlet})
        assert calls == ["build", "emit"]

    def test_no_outlet_events_still_builds(self, monkeypatch):
        calls = []
        monkeypatch.setattr(spark_ingestion, "_export_dimensions", lambda: calls.append("build"))
        spark_ingestion._export_dimensions_with_outlet()
        assert calls == ["build"]

    def test_build_failure_does_not_emit(self, monkeypatch):
        emitted = []

        def boom():
            raise RuntimeError("dimension build failed")

        monkeypatch.setattr(spark_ingestion, "_export_dimensions", boom)
        outlet = SimpleNamespace(emit=lambda: emitted.append(True))
        with pytest.raises(RuntimeError):
            spark_ingestion._export_dimensions_with_outlet(outlet_events={spark_ingestion.WAREHOUSE_LOADED: outlet})
        assert emitted == []


@pytest.fixture
def gate(monkeypatch):
    """A real operator instance usable outside a TaskInstance."""
    monkeypatch.setattr(spark_ingestion._ConfGatedSparkSubmit, "allow_nested_operators", True, raising=False)
    return spark_ingestion.silver_backfill


class TestConfGatedSparkSubmit:
    def test_skips_without_conf(self, gate):
        with pytest.raises(AirflowSkipException):
            gate.execute(context={"dag_run": SimpleNamespace(conf={})})

    def test_skips_without_dag_run(self, gate):
        with pytest.raises(AirflowSkipException):
            gate.execute(context={})

    @pytest.mark.parametrize(
        "conf",
        [{"start_date": "2009-10-24"}, {"end_date": "2009-10-26"}],
    )
    def test_skips_when_only_one_boundary_present(self, gate, conf):
        with pytest.raises(AirflowSkipException):
            gate.execute(context={"dag_run": SimpleNamespace(conf=conf)})

    def test_submits_when_both_boundaries_present(self, gate, monkeypatch):
        submitted = []
        monkeypatch.setattr(
            spark_ingestion.SparkSubmitOperator,
            "execute",
            lambda self, context: submitted.append(context),
        )
        context = {"dag_run": SimpleNamespace(conf={"start_date": "2009-10-24", "end_date": "2009-10-26"})}
        gate.execute(context=context)
        assert submitted == [context]


class TestAzureDimensionBuilders:
    """The Azure builders are SQL-heavy; assert the control flow, not the SQL."""

    def test_build_dim_geolocation_runs_ddl_then_scd2_and_commits(self):
        from unittest.mock import MagicMock

        cursor, conn = MagicMock(), MagicMock()
        spark_ingestion_azure._build_dim_geolocation(cursor, conn)
        assert cursor.execute.call_count == 2
        upload = cursor.execute.call_args_list[1].args[0]
        assert "MERGE dbo.dim_geolocation" in upload
        conn.commit.assert_called_once()

    def test_build_dim_useragent_skips_merge_when_no_rows(self):
        from unittest.mock import MagicMock

        cursor, conn = MagicMock(), MagicMock()
        cursor.fetchall.return_value = []
        spark_ingestion_azure._build_dim_useragent(cursor, conn)
        assert cursor.execute.call_count == 2
        conn.commit.assert_called_once()

    def test_build_dim_useragent_merges_parsed_rows(self, monkeypatch):
        from unittest.mock import MagicMock

        merged = []
        monkeypatch.setattr(
            spark_ingestion_azure,
            "_parse_user_agent_rows",
            lambda rows: [("hash", "ua", "Browser", "Chrome", "1", "Windows", "Desktop")],
        )
        monkeypatch.setattr(
            spark_ingestion_azure,
            "_merge_useragent_rows",
            lambda cursor, data: merged.append(data),
        )
        cursor, conn = MagicMock(), MagicMock()
        cursor.fetchall.return_value = [("Mozilla/5.0",)]
        spark_ingestion_azure._build_dim_useragent(cursor, conn)
        assert len(merged) == 1
        conn.commit.assert_called_once()


class TestEmitOutlets:
    def test_emits_each_dataset(self):
        emitted = []
        ds_a, ds_b = object(), object()
        events = {
            ds_a: SimpleNamespace(emit=lambda: emitted.append("a")),
            ds_b: SimpleNamespace(emit=lambda: emitted.append("b")),
        }
        spark_ingestion_azure._emit_outlets({"outlet_events": events}, ds_a, ds_b)
        assert emitted == ["a", "b"]

    def test_no_context_is_a_noop(self):
        spark_ingestion_azure._emit_outlets({}, object())
