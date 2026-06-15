"""
Unit tests for Databricks UC Gold export (``03_export_warehouse.py``).

Tests cover:

* ``get_source_files_in_table`` — loaded-file discovery from UC table
* ``ensure_gold_tables`` — DDL for gold and tracking UC tables
* ``insert_tracking_records`` — tracking table idempotency records
* ``run`` — end-to-end Silver → Gold export pipeline

All PySpark-dependent tests use a local ``spark`` fixture that does **not**
require the Delta Lake JAR — ``delta`` format calls are transparently
remapped to ``parquet`` so the pipeline logic can be verified on any
Spark distribution.
"""

import importlib
import os
import sys
import tempfile

import pytest

pytest.importorskip("pyspark")
from pyspark.sql import SparkSession

# ── Monkey-patch DataFrameWriter so ``format("delta")`` → ``format("parquet")`` ──
from pyspark.sql.readwriter import DataFrameWriter

_orig_format = DataFrameWriter.format


def _delta_to_parquet(self, source):
    return _orig_format(self, "parquet" if source == "delta" else source)


DataFrameWriter.format = _delta_to_parquet

# Also patch SparkSession.sql to replace USING DELTA with USING PARQUET
_orig_sql = SparkSession.sql


def _no_delta_ddl(self, sql_query, *args, **kwargs):
    return _orig_sql(self, sql_query.replace("USING DELTA", "USING PARQUET"), *args, **kwargs)


SparkSession.sql = _no_delta_ddl

# ── Import the module under test ─────────────────────────────────────
_TEST_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.join(_TEST_DIR, "..")

_NESTED_DB = os.path.join(_PROJECT_ROOT, "airflow", "spark", "databricks")
_FLAT_DB = os.path.join(_PROJECT_ROOT, "spark", "databricks")

for _db_path in (_NESTED_DB, _FLAT_DB):
    if os.path.isdir(_db_path) and _db_path not in sys.path:
        sys.path.insert(0, _db_path)

export = importlib.import_module("03_export_warehouse")


# ── Local spark fixture (no addPyFile — the scripts are self-contained) ──────
@pytest.fixture
def spark():
    """Create a minimal local SparkSession for testing pipeline logic.

    Uses a unique warehouse directory so tables from one test do not
    collide with another test on ``saveAsTable``.
    """
    warehouse_dir = tempfile.mkdtemp(prefix="spark_wh_")
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("W3C_ETL_UC_Test")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.warehouse.dir", warehouse_dir)
        .getOrCreate()
    )
    yield session
    session.stop()


# ══════════════════════════════════════════════════════════════════════
#  1. get_source_files_in_table — UC table source-file discovery
# ══════════════════════════════════════════════════════════════════════


class TestGetSourceFilesInTable:
    """Verify source-file discovery from a Unity Catalog table."""

    def test_returns_empty_set_when_table_does_not_exist(self, spark):
        result = export.get_source_files_in_table(spark, "nonexistent.table")
        assert result == set()

    def test_returns_set_of_source_files(self, spark):
        table_name = "test_export_sources"
        spark.createDataFrame(
            [("f1.log",), ("f2.log",), ("f1.log",)],
            schema="source_file string",
        ).write.mode("overwrite").saveAsTable(table_name)

        result = export.get_source_files_in_table(spark, table_name)
        assert result == {"f1.log", "f2.log"}

        spark.sql(f"DROP TABLE IF EXISTS {table_name}")


# ══════════════════════════════════════════════════════════════════════
#  2. ensure_gold_tables — DDL for gold + tracking tables
# ══════════════════════════════════════════════════════════════════════


class TestEnsureGoldTables:
    """Verify the DDL statements create both gold and tracking tables."""

    GOLD_TABLE = "test_gold_ensure"
    TRACKING_TABLE = "test_tracking_ensure"

    @pytest.fixture(autouse=True)
    def _patch_uc_paths(self):
        orig_gold = export.GOLD_UC_PATH
        orig_tracking = export.TRACKING_UC_PATH
        export.GOLD_UC_PATH = self.GOLD_TABLE
        export.TRACKING_UC_PATH = self.TRACKING_TABLE
        yield
        export.GOLD_UC_PATH = orig_gold
        export.TRACKING_UC_PATH = orig_tracking

    def test_creates_gold_table(self, spark):
        export.ensure_gold_tables(spark)
        df = spark.table(self.GOLD_TABLE)
        assert df.count() == 0

    def test_creates_tracking_table(self, spark):
        export.ensure_gold_tables(spark)
        df = spark.table(self.TRACKING_TABLE)
        assert df.count() == 0

    def test_gold_table_has_expected_columns(self, spark):
        export.ensure_gold_tables(spark)
        columns = {f.name for f in spark.table(self.GOLD_TABLE).schema.fields}
        expected = {
            "log_date",
            "log_time",
            "server_ip",
            "method",
            "uri_stem",
            "uri_query",
            "client_ip",
            "user_agent",
            "cookie",
            "referrer",
            "status",
            "sub_status",
            "win32_status",
            "bytes_sent",
            "bytes_recv",
            "server_port",
            "username",
            "time_taken",
            "source_file",
            "page_category",
            "referrer_domain",
            "traffic_type",
            "is_crawler",
            "size_band",
        }
        assert columns == expected, f"Missing: {expected - columns}"

    def test_tracking_table_has_source_file_column(self, spark):
        export.ensure_gold_tables(spark)
        columns = {f.name for f in spark.table(self.TRACKING_TABLE).schema.fields}
        assert "source_file" in columns

    def test_idempotent(self, spark):
        export.ensure_gold_tables(spark)
        export.ensure_gold_tables(spark)  # second call — no error
        assert spark.table(self.GOLD_TABLE).schema is not None
        assert spark.table(self.TRACKING_TABLE).schema is not None


# ══════════════════════════════════════════════════════════════════════
#  3. insert_tracking_records — tracking table writes
# ══════════════════════════════════════════════════════════════════════


class TestInsertTrackingRecords:
    """Verify tracking record insertion into the tracking table."""

    TRACKING_TABLE = "test_tracking_insert"

    @pytest.fixture(autouse=True)
    def _patch_tracking_path(self):
        original = export.TRACKING_UC_PATH
        export.TRACKING_UC_PATH = self.TRACKING_TABLE
        yield
        export.TRACKING_UC_PATH = original

    def test_inserts_single_file(self, spark):
        # Create the tracking table first (USING DELTA → USING PARQUET via monkeypatch)
        spark.sql(f"CREATE TABLE IF NOT EXISTS {self.TRACKING_TABLE} (source_file STRING) USING DELTA")

        export.insert_tracking_records(spark, {"file1.log"})

        rows = spark.table(self.TRACKING_TABLE).collect()
        assert len(rows) == 1
        assert rows[0].source_file == "file1.log"

    def test_inserts_multiple_files(self, spark):
        spark.sql(f"CREATE TABLE IF NOT EXISTS {self.TRACKING_TABLE} (source_file STRING) USING DELTA")

        export.insert_tracking_records(spark, {"f1.log", "f2.log", "f3.log"})

        rows = spark.table(self.TRACKING_TABLE).collect()
        assert len(rows) == 3
        assert {r.source_file for r in rows} == {"f1.log", "f2.log", "f3.log"}

    def test_empty_set_does_nothing(self, spark):
        spark.sql(f"CREATE TABLE IF NOT EXISTS {self.TRACKING_TABLE} (source_file STRING) USING DELTA")

        export.insert_tracking_records(spark, set())

        rows = spark.table(self.TRACKING_TABLE).collect()
        assert len(rows) == 0


# ══════════════════════════════════════════════════════════════════════
#  4. run — end-to-end export pipeline
# ══════════════════════════════════════════════════════════════════════


class TestRunExport:
    """Verify the full Silver → Gold export pipeline."""

    SILVER_TABLE = "test_export_run_silver"
    GOLD_TABLE = "test_export_run_gold"
    TRACKING_TABLE = "test_export_run_tracking"

    @pytest.fixture(autouse=True)
    def _patch_uc_paths(self):
        orig_silver = export.SILVER_UC_PATH
        orig_gold = export.GOLD_UC_PATH
        orig_tracking = export.TRACKING_UC_PATH
        export.SILVER_UC_PATH = self.SILVER_TABLE
        export.GOLD_UC_PATH = self.GOLD_TABLE
        export.TRACKING_UC_PATH = self.TRACKING_TABLE
        yield
        export.SILVER_UC_PATH = orig_silver
        export.GOLD_UC_PATH = orig_gold
        export.TRACKING_UC_PATH = orig_tracking

    def _create_silver_data(self, spark, rows: list[dict]):
        """Populate the mock silver table."""
        spark.createDataFrame(rows).write.mode("overwrite").saveAsTable(self.SILVER_TABLE)

    def test_creates_gold_table_and_writes_data(self, spark):
        """Gold table has 24 columns (no separate geo fields)."""
        self._create_silver_data(
            spark,
            [
                {
                    "log_date": "2009-10-24",
                    "log_time": "01:40:40",
                    "server_ip": "134.36.36.75",
                    "method": "GET",
                    "uri_stem": "/",
                    "uri_query": "",
                    "client_ip": "1.2.3.4",
                    "user_agent": "Mozilla/4.0",
                    "cookie": "",
                    "referrer": "-",
                    "status": 200,
                    "sub_status": 0,
                    "win32_status": 0,
                    "bytes_sent": 500,
                    "bytes_recv": 100,
                    "server_port": 80,
                    "username": "-",
                    "time_taken": 100,
                    "source_file": "test.log",
                    "page_category": "Homepage",
                    "referrer_domain": "Direct",
                    "traffic_type": "Direct",
                    "is_crawler": "false",
                    "size_band": "Tiny (<1KB)",
                },
            ],
        )

        export.run(spark)

        gold_df = spark.table(self.GOLD_TABLE)
        assert gold_df.count() == 1

        row = gold_df.collect()[0]
        assert row.uri_stem == "/"
        assert row.page_category == "Homepage"

    def test_updates_tracking_table(self, spark):
        self._create_silver_data(
            spark,
            [
                {
                    "log_date": "2009-10-24",
                    "log_time": "01:40:40",
                    "server_ip": "1.2.3.4",
                    "method": "GET",
                    "uri_stem": "/",
                    "uri_query": "",
                    "client_ip": "1.2.3.4",
                    "user_agent": "UA",
                    "cookie": "",
                    "referrer": "-",
                    "status": 200,
                    "sub_status": 0,
                    "win32_status": 0,
                    "bytes_sent": 100,
                    "bytes_recv": 10,
                    "server_port": 80,
                    "username": "-",
                    "time_taken": 50,
                    "source_file": "tracked.log",
                    "page_category": "Content",
                    "referrer_domain": "Direct",
                    "traffic_type": "Direct",
                    "is_crawler": "false",
                    "size_band": "Zero",
                },
            ],
        )

        export.run(spark)

        tracking = spark.table(self.TRACKING_TABLE)
        assert {r.source_file for r in tracking.collect()} == {"tracked.log"}

    def test_incremental_skip_already_exported(self, spark):
        """Files already in the tracking table are skipped on re-run."""
        self._create_silver_data(
            spark,
            [
                {
                    "log_date": "2009-10-24",
                    "log_time": "01:40:40",
                    "server_ip": "1.2.3.4",
                    "method": "GET",
                    "uri_stem": "/",
                    "uri_query": "",
                    "client_ip": "1.2.3.4",
                    "user_agent": "UA",
                    "cookie": "",
                    "referrer": "-",
                    "status": 200,
                    "sub_status": 0,
                    "win32_status": 0,
                    "bytes_sent": 100,
                    "bytes_recv": 10,
                    "server_port": 80,
                    "username": "-",
                    "time_taken": 50,
                    "source_file": "dup.log",
                    "page_category": "Content",
                    "referrer_domain": "Direct",
                    "traffic_type": "Direct",
                    "is_crawler": "false",
                    "size_band": "Zero",
                },
            ],
        )

        export.run(spark)
        assert spark.table(self.GOLD_TABLE).count() == 1

        export.run(spark)
        assert spark.table(self.GOLD_TABLE).count() == 1  # no duplicate

    def test_empty_silver_returns_early(self, spark):
        """Run with no silver data should not fail."""
        export.run(spark)  # silver table doesn't exist yet
        # Should handle gracefully — no error expected

    def test_handles_missing_silver_table(self, spark):
        """When silver table does not exist, run should log and return."""
        export.run(spark)
        # The pipeline catches exceptions from spark.table(SILVER_UC_PATH)
        # and logs an error instead of crashing.
