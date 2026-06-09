"""
DAG integrity tests for the pipeline.

Verifies that both DAG files (``spark_ingestion.py`` and ``dbt_marts.py``)
parse correctly under Airflow and match expected task counts and data-flow
contracts (Dataset outlets / triggers).

Running
-------
These tests require Airflow to be installed (they use ``airflow.models.DagBag``).
They are marked ``@pytest.mark.dag_integrity`` and are skipped by default:

    # Run all non-integration tests (skips dag_integrity):
    pytest tests/ -v

    # Run DAG integrity tests explicitly:
    pytest tests/test_dag_integrity.py -v -m dag_integrity

    # Inside the Docker Airflow container:
    docker compose exec airflow-scheduler \\
        python -m pytest tests/test_dag_integrity.py -v --tb=short
"""

import pytest

# DAG folder on the Airflow worker (also works when running from project root
# via ``PYTHONPATH`` pointing at the ``dags`` directory).
_DAG_FOLDER = "/opt/airflow/dags/w3c"


@pytest.mark.dag_integrity
class TestSparkIngestionDAG:
    """Verify the ``w3c_spark_ingestion`` DAG (bronze → silver → export)."""

    def test_spark_ingestion_imports(self):
        """Verify spark_ingestion DAG imports and parses with 4 tasks."""
        from airflow.models import DagBag

        dag_bag = DagBag(dag_folder=_DAG_FOLDER, include_examples=False)
        dag = dag_bag.get_dag(dag_id="w3c_spark_ingestion")
        assert dag is not None, "w3c_spark_ingestion DAG not found in DagBag. Import errors: %s" % dag_bag.import_errors
        # Expected: bronze_ingestion, silver_enrichment, export_warehouse, export_dimensions
        assert len(dag.tasks) == 4, (
            f"Expected 4 tasks (bronze_ingestion, silver_enrichment, "
            f"export_warehouse, export_dimensions), got {len(dag.tasks)}: "
            f"{[t.task_id for t in dag.tasks]}"
        )

    def test_spark_ingestion_task_ids(self):
        """Verify the spark_ingestion DAG has the expected task IDs."""
        from airflow.models import DagBag

        dag_bag = DagBag(dag_folder=_DAG_FOLDER, include_examples=False)
        dag = dag_bag.get_dag(dag_id="w3c_spark_ingestion")
        assert dag is not None

        task_ids = {t.task_id for t in dag.tasks}
        expected = {
            "bronze_ingestion",
            "silver_enrichment",
            "export_warehouse",
            "export_dimensions",
        }
        missing = expected - task_ids
        extra = task_ids - expected
        assert not missing, f"Missing task(s): {missing}"
        assert not extra, f"Unexpected task(s): {extra}"

    def test_spark_ingestion_linear_dependency(self):
        """Verify tasks form a single linear chain: bronze >> silver >> export_wh >> export_dim."""
        from airflow.models import DagBag

        dag_bag = DagBag(dag_folder=_DAG_FOLDER, include_examples=False)
        dag = dag_bag.get_dag(dag_id="w3c_spark_ingestion")
        assert dag is not None

        # Identify upstream → downstream relationships
        downstream_map = {}
        for task in dag.tasks:
            downstream_map[task.task_id] = {t.task_id for t in task.downstream_list}

        # Each task should have exactly one downstream (except the last)
        assert downstream_map["bronze_ingestion"] == {"silver_enrichment"}
        assert downstream_map["silver_enrichment"] == {"export_warehouse"}
        assert downstream_map["export_warehouse"] == {"export_dimensions"}
        assert downstream_map["export_dimensions"] == set()

    def test_spark_ingestion_has_dataset_outlet(self):
        """Verify the spark_ingestion DAG emits Dataset("postgres://postgres:5432/w3c_warehouse/public/raw_enriched_loaded")."""
        from airflow.models import DagBag

        dag_bag = DagBag(dag_folder=_DAG_FOLDER, include_examples=False)
        dag = dag_bag.get_dag(dag_id="w3c_spark_ingestion")
        assert dag is not None

        # Find the export_dimensions task (it has the outlet)
        export_dim_task = None
        for task in dag.tasks:
            if task.task_id == "export_dimensions":
                export_dim_task = task
                break

        assert export_dim_task is not None, "export_dimensions task not found"
        # Check outlets attribute
        outlets = getattr(export_dim_task, "outlets", [])
        assert len(outlets) > 0, "export_dimensions task has no outlets"

        from airflow.datasets import Dataset

        target = Dataset("postgres://postgres:5432/w3c_warehouse/public/raw_enriched_loaded")
        assert target in outlets, (
            f"Expected Dataset('postgres://postgres:5432/w3c_warehouse/public/raw_enriched_loaded') in outlets, got {outlets}"
        )

    def test_spark_ingestion_has_no_import_errors(self):
        """Verify the DAG file has zero import errors in DagBag."""
        from airflow.models import DagBag

        dag_bag = DagBag(dag_folder=_DAG_FOLDER, include_examples=False)

        # Trigger the parse by accessing dag_bag.dags or dag_bag.import_errors
        _ = dag_bag.dags

        assert len(dag_bag.import_errors) == 0, f"DAG import errors found: {dag_bag.import_errors}"


@pytest.mark.dag_integrity
class TestDBTMartsDAG:
    """Verify the ``dbt_marts`` DAG (dataset-triggered dbt pipeline)."""

    def test_dbt_marts_imports(self):
        """Verify dbt_marts DAG imports and parses with 5 tasks."""
        from airflow.models import DagBag

        dag_bag = DagBag(dag_folder=_DAG_FOLDER, include_examples=False)
        dag = dag_bag.get_dag(dag_id="w3c_dbt_marts")
        assert dag is not None, "dbt_marts DAG not found in DagBag. Import errors: %s" % dag_bag.import_errors
        # Expected: dbt_deps, dbt_run, dbt_test, dbt_docs, export_csv
        assert len(dag.tasks) == 5, (
            f"Expected 5 tasks (dbt_deps, dbt_run, dbt_test, dbt_docs, export_csv), "
            f"got {len(dag.tasks)}: {[t.task_id for t in dag.tasks]}"
        )

    def test_dbt_marts_task_ids(self):
        """Verify the dbt_marts DAG has the expected task IDs."""
        from airflow.models import DagBag

        dag_bag = DagBag(dag_folder=_DAG_FOLDER, include_examples=False)
        dag = dag_bag.get_dag(dag_id="w3c_dbt_marts")
        assert dag is not None

        task_ids = {t.task_id for t in dag.tasks}
        expected = {"dbt_deps", "dbt_run", "dbt_test", "dbt_docs", "export_csv"}
        missing = expected - task_ids
        extra = task_ids - expected
        assert not missing, f"Missing task(s): {missing}"
        assert not extra, f"Unexpected task(s): {extra}"

    def test_dbt_marts_linear_dependency(self):
        """Verify tasks form: dbt_deps >> dbt_run >> dbt_test >> dbt_docs >> export_csv."""
        from airflow.models import DagBag

        dag_bag = DagBag(dag_folder=_DAG_FOLDER, include_examples=False)
        dag = dag_bag.get_dag(dag_id="w3c_dbt_marts")
        assert dag is not None

        downstream_map = {}
        for task in dag.tasks:
            downstream_map[task.task_id] = {t.task_id for t in task.downstream_list}

        assert downstream_map["dbt_deps"] == {"dbt_run"}
        assert downstream_map["dbt_run"] == {"dbt_test"}
        assert downstream_map["dbt_test"] == {"dbt_docs"}
        assert downstream_map["dbt_docs"] == {"export_csv"}
        assert downstream_map["export_csv"] == set()

    def test_dbt_marts_is_dataset_triggered(self):
        """Verify dbt_marts is triggered by Dataset("postgres://postgres:5432/w3c_warehouse/public/raw_enriched_loaded")."""
        from airflow.datasets import Dataset
        from airflow.models import DagBag

        dag_bag = DagBag(dag_folder=_DAG_FOLDER, include_examples=False)
        dag = dag_bag.get_dag(dag_id="w3c_dbt_marts")
        assert dag is not None

        # Extract datasets from the timetable
        timetable = dag.timetable
        cond = getattr(timetable, "dataset_condition", None)
        dbt_inlet_datasets = set()
        if cond is not None:
            if hasattr(cond, "objects"):
                for obj in cond.objects:
                    if isinstance(obj, Dataset):
                        dbt_inlet_datasets.add(obj)
            elif isinstance(cond, Dataset):
                dbt_inlet_datasets.add(cond)

        target = Dataset("postgres://postgres:5432/w3c_warehouse/public/raw_enriched_loaded")
        assert target in dbt_inlet_datasets, (
            f"Expected Dataset('postgres://postgres:5432/w3c_warehouse/public/raw_enriched_loaded') in schedule, got {dbt_inlet_datasets}"
        )

    def test_dbt_marts_has_no_import_errors(self):
        """Verify the DAG file has zero import errors in DagBag."""
        from airflow.models import DagBag

        dag_bag = DagBag(dag_folder=_DAG_FOLDER, include_examples=False)
        _ = dag_bag.dags
        assert len(dag_bag.import_errors) == 0, f"DAG import errors found: {dag_bag.import_errors}"

    def test_export_csv_generates_correct_script(self):
        """Verify the export CSV script covers all 18 tables."""
        from airflow.models import DagBag

        dag_bag = DagBag(dag_folder=_DAG_FOLDER, include_examples=False)
        dag = dag_bag.get_dag(dag_id="w3c_dbt_marts")
        assert dag is not None

        export_task = None
        for task in dag.tasks:
            if task.task_id == "export_csv":
                export_task = task
                break

        assert export_task is not None, "export_csv task not found"
        bash_command = export_task.bash_command

        # Should reference all 16 tables + 2 public tables = 18 total
        # (STAGING_TABLES: 10, MART_TABLES: 6, PUBLIC_TABLES: 2)
        for table in [
            "dbt_staging.fact_webrequest",
            "dbt_staging.dim_date",
            "dbt_staging.dim_time",
            "dbt_staging.dim_page",
            "dbt_staging.dim_status",
            "dbt_staging.dim_referrer",
            "dbt_staging.dim_method",
            "dbt_staging.dim_visitortype",
            "dbt_staging.dim_visit_buckets",
            "dbt_staging.crawler_ips",
            "dbt_marts.mart_page_performance",
            "dbt_marts.mart_daily_aggregates",
            "dbt_marts.mart_crawler_analysis",
            "dbt_marts.mart_browser_analysis",
            "dbt_marts.mart_timeofday_analysis",
            "dbt_marts.mart_country_browser_share",
            "public.dim_geolocation",
            "public.dim_useragent",
        ]:
            assert table in bash_command, f"Table {table} not found in export_csv bash_command"


@pytest.mark.dag_integrity
class TestDAGDataContract:
    """Verify the data contract between the two DAGs."""

    def test_spark_outlet_matches_dbt_inlet(self):
        """The Dataset emitted by spark_ingestion should be the same as dbt_marts consumes."""
        from airflow.datasets import Dataset
        from airflow.models import DagBag

        dag_bag = DagBag(dag_folder=_DAG_FOLDER, include_examples=False)

        # Get spark DAG outlet
        spark_dag = dag_bag.get_dag(dag_id="w3c_spark_ingestion")
        assert spark_dag is not None

        spark_outlet_datasets = set()
        for task in spark_dag.tasks:
            for outlet in getattr(task, "outlets", []):
                spark_outlet_datasets.add(outlet)

        assert len(spark_outlet_datasets) > 0, "spark_ingestion has no Dataset outlets"

        # Get dbt DAG inlet (schedule = Dataset list or timetable collection)
        dbt_dag = dag_bag.get_dag(dag_id="w3c_dbt_marts")
        assert dbt_dag is not None

        dbt_inlet_datasets = set()
        timetable = dbt_dag.timetable
        cond = getattr(timetable, "dataset_condition", None)
        if cond is not None:
            if hasattr(cond, "objects"):
                for obj in cond.objects:
                    if isinstance(obj, Dataset):
                        dbt_inlet_datasets.add(obj)
            elif isinstance(cond, Dataset):
                dbt_inlet_datasets.add(cond)

        assert len(dbt_inlet_datasets) > 0, "dbt_marts has no Dataset inlets"

        # The contract: what spark emits, dbt consumes
        assert spark_outlet_datasets == dbt_inlet_datasets, (
            f"Mismatch! Spark outlets: {spark_outlet_datasets}, dbt inlets: {dbt_inlet_datasets}"
        )


@pytest.mark.dag_integrity
class TestSparkIngestionAzureDAG:
    """Verify the ``w3c_spark_ingestion_azure`` DAG (bronze/silver JDBC → export)."""

    def test_azure_dag_imports(self):
        """Verify spark_ingestion_azure DAG imports and parses with 2 tasks."""
        from airflow.models import DagBag

        dag_bag = DagBag(dag_folder=_DAG_FOLDER, include_examples=False)
        dag = dag_bag.get_dag(dag_id="w3c_spark_ingestion_azure")
        assert dag is not None, (
            "w3c_spark_ingestion_azure DAG not found in DagBag. Import errors: %s" % dag_bag.import_errors
        )
        assert len(dag.tasks) == 2, (
            f"Expected 2 tasks (bronze_silver_jdbc_pipeline, export_dimensions), "
            f"got {len(dag.tasks)}: {[t.task_id for t in dag.tasks]}"
        )

    def test_azure_dag_task_ids(self):
        """Verify the spark_ingestion_azure DAG has the expected task IDs."""
        from airflow.models import DagBag

        dag_bag = DagBag(dag_folder=_DAG_FOLDER, include_examples=False)
        dag = dag_bag.get_dag(dag_id="w3c_spark_ingestion_azure")
        assert dag is not None

        task_ids = {t.task_id for t in dag.tasks}
        expected = {
            "bronze_silver_jdbc_pipeline",
            "export_dimensions",
        }
        missing = expected - task_ids
        extra = task_ids - expected
        assert not missing, f"Missing task(s): {missing}"
        assert not extra, f"Unexpected task(s): {extra}"

    def test_azure_dag_linear_dependency(self):
        """Verify tasks form: bronze_silver_jdbc_pipeline >> export_dimensions."""
        from airflow.models import DagBag

        dag_bag = DagBag(dag_folder=_DAG_FOLDER, include_examples=False)
        dag = dag_bag.get_dag(dag_id="w3c_spark_ingestion_azure")
        assert dag is not None

        downstream_map = {}
        for task in dag.tasks:
            downstream_map[task.task_id] = {t.task_id for t in task.downstream_list}

        assert downstream_map["bronze_silver_jdbc_pipeline"] == {"export_dimensions"}
        assert downstream_map["export_dimensions"] == set()

    def test_azure_dag_has_dataset_outlet(self):
        """Verify the spark_ingestion_azure DAG emits Dataset("mssql://azure-sql/dbo/raw_enriched_loaded")."""
        from airflow.models import DagBag

        dag_bag = DagBag(dag_folder=_DAG_FOLDER, include_examples=False)
        dag = dag_bag.get_dag(dag_id="w3c_spark_ingestion_azure")
        assert dag is not None

        export_dim_task = None
        for task in dag.tasks:
            if task.task_id == "export_dimensions":
                export_dim_task = task
                break

        assert export_dim_task is not None, "export_dimensions task not found"
        outlets = getattr(export_dim_task, "outlets", [])
        assert len(outlets) > 0, "export_dimensions task has no outlets"

        from airflow.datasets import Dataset

        target = Dataset("mssql://azure-sql/dbo/raw_enriched_loaded")
        assert target in outlets, (
            f"Expected Dataset('mssql://azure-sql/dbo/raw_enriched_loaded') in outlets, got {outlets}"
        )

    def test_azure_dag_has_no_import_errors(self):
        """Verify the DAG file has zero import errors in DagBag."""
        from airflow.models import DagBag

        dag_bag = DagBag(dag_folder=_DAG_FOLDER, include_examples=False)
        _ = dag_bag.dags
        assert len(dag_bag.import_errors) == 0, f"DAG import errors found: {dag_bag.import_errors}"
