"""
Unit tests for ``export_warehouse.py`` — the Spark JDBC export job.

Tests the five key areas of the Silver → PostgreSQL pipeline:
1. DDL generation (extracted from source via AST — no Spark required)
2. Argument parsing / JDBC URL construction (own argparse — no Spark)
3. Schema type casting via ``apply_type_casts()`` (requires spark fixture)
4. Idempotency tracking via ``get_loaded_source_files()`` (mocked JDBC)
5. Error handling — tracking table is NOT updated on write failure

Tests that require PySpark are guarded with ``pytest.importorskip("pyspark")``
so they only run when PySpark is available.

Usage:
    pytest tests/test_export_warehouse.py -v --tb=short

    # With PySpark installed (in Docker or with pyspark>=4.0):
    PYTHONPATH=airflow/spark/jobs:$PYTHONPATH pytest tests/test_export_warehouse.py -v
"""

import ast
import os
from unittest.mock import ANY, MagicMock, PropertyMock, patch

import pytest

# ── Helpers ───────────────────────────────────────────────────────────────────

# Path to the source file for AST-based constant extraction
_EXPORT_WAREHOUSE_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "spark", "jobs", "export_warehouse.py")
)
if not os.path.exists(_EXPORT_WAREHOUSE_PATH):
    _EXPORT_WAREHOUSE_PATH = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "airflow", "spark", "jobs", "export_warehouse.py")
    )


def _get_module_constant(name: str) -> str | None:
    """Extract a top-level string constant from ``export_warehouse.py`` via AST.

    This avoids importing the module (which requires PySpark) for tests
    that only need to verify the DDL strings.
    """
    with open(_EXPORT_WAREHOUSE_PATH) as f:
        tree = ast.parse(f.read())
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == name:
                    if isinstance(node.value, ast.Constant) and isinstance(node.value.value, str):
                        return node.value.value
    return None


def _requires_pyspark():
    """Skip the test if PySpark is not installed."""
    return pytest.importorskip("pyspark", reason="PySpark is not installed")


# ═══════════════════════════════════════════════════════════════════════════
#  1. DDL Generation — pure string checks (no Spark needed)
# ═══════════════════════════════════════════════════════════════════════════


class TestDDLGeneration:
    """Verify CREATE TABLE IF NOT EXISTS DDL is correctly generated.

    These tests extract constants from the source file using AST parsing,
    so they do NOT require PySpark or Airflow to be installed.
    """

    def test_raw_enriched_ddl_has_create_statement(self):
        ddl = _get_module_constant("RAW_ENRICHED_DDL")
        assert ddl is not None, "RAW_ENRICHED_DDL constant not found in source"
        assert "CREATE TABLE IF NOT EXISTS public.raw_enriched" in ddl

    def test_raw_enriched_ddl_has_type_casts(self):
        ddl = _get_module_constant("RAW_ENRICHED_DDL")
        assert ddl is not None
        # Verify PostgreSQL-native types match the cast transformations
        assert "is_crawler BOOLEAN" in ddl
        assert "bytes_sent BIGINT" in ddl
        assert "bytes_recv BIGINT" in ddl
        assert "time_taken BIGINT" in ddl
        assert "latitude DOUBLE PRECISION" in ddl
        assert "longitude DOUBLE PRECISION" in ddl

    def test_raw_enriched_ddl_has_all_35_columns(self):
        ddl = _get_module_constant("RAW_ENRICHED_DDL")
        assert ddl is not None
        lines = [line.strip() for line in ddl.strip().split("\n")]
        col_lines = [
            line
            for line in lines
            if line and not line.startswith("CREATE") and line != "(" and line != ");" and not line.startswith(")")
        ]
        col_names = [line.rstrip(",").split()[0] for line in col_lines if line]
        assert len(col_names) == 35, f"Expected 35 columns, got {len(col_names)}: {col_names}"

    def test_tracking_ddl_has_source_file_pk(self):
        ddl = _get_module_constant("TRACKING_DDL")
        assert ddl is not None
        assert "CREATE TABLE IF NOT EXISTS public.raw_enriched_loaded" in ddl
        assert "source_file TEXT PRIMARY KEY" in ddl

    def test_tracking_ddl_simple_schema(self):
        ddl = _get_module_constant("TRACKING_DDL")
        assert ddl is not None
        cols = [
            line.strip().rstrip(",")
            for line in ddl.strip().split("\n")
            if line.strip() and "CREATE" not in line and line.strip() not in ("(", ");", "")
        ]
        assert len(cols) == 1
        assert "source_file" in cols[0]

    def test_both_ddl_constants_exist(self):
        """Ensure both DDL constants are defined in the source."""
        raw = _get_module_constant("RAW_ENRICHED_DDL")
        track = _get_module_constant("TRACKING_DDL")
        assert raw is not None, "RAW_ENRICHED_DDL not found"
        assert track is not None, "TRACKING_DDL not found"


# ═══════════════════════════════════════════════════════════════════════════
#  2. JDBC URL Construction — argparse smoke tests (no Spark needed)
# ═══════════════════════════════════════════════════════════════════════════


class TestJDBCArgumentParsing:
    """Verify CLI argument parsing produces correct JDBC parameters.

    These tests create their own ArgumentParser (mirroring the one in
    ``main()``) so they do NOT require PySpark.
    """

    def _make_parser(self):
        """Replicate the ArgumentParser from export_warehouse.py."""
        import argparse

        parser = argparse.ArgumentParser(description="Export Warehouse: Silver → PostgreSQL via Spark JDBC")
        parser.add_argument(
            "--delta-dir",
            default="/opt/spark/delta",
            help="Root Delta Lake directory (contains silver/).",
        )
        parser.add_argument("--jdbc-url", required=True)
        parser.add_argument("--jdbc-user", required=True)
        parser.add_argument("--jdbc-password", required=True)
        parser.add_argument(
            "--jdbc-driver",
            default="org.postgresql.Driver",
            help="JDBC driver class name.",
        )
        return parser

    def test_default_values(self):
        parser = self._make_parser()
        args = parser.parse_args([
            "--jdbc-url",
            "jdbc:postgresql://postgres:5432/w3c_warehouse",
            "--jdbc-user",
            "airflow",
            "--jdbc-password",
            "s3cret",
        ])
        assert args.jdbc_url == "jdbc:postgresql://postgres:5432/w3c_warehouse"
        assert args.jdbc_user == "airflow"
        assert args.jdbc_password == "s3cret"
        assert args.jdbc_driver == "org.postgresql.Driver"  # default
        assert args.delta_dir == "/opt/spark/delta"  # default

    def test_custom_delta_dir(self):
        parser = self._make_parser()
        args = parser.parse_args([
            "--delta-dir",
            "/custom/delta",
            "--jdbc-url",
            "jdbc:postgresql://rds:5432/prod",
            "--jdbc-user",
            "admin",
            "--jdbc-password",
            "pass",
            "--jdbc-driver",
            "org.postgresql.Driver",
        ])
        assert args.delta_dir == "/custom/delta"
        assert args.jdbc_url == "jdbc:postgresql://rds:5432/prod"

    def test_custom_jdbc_driver(self):
        parser = self._make_parser()
        args = parser.parse_args([
            "--jdbc-url",
            "jdbc:postgresql://host:5432/db",
            "--jdbc-user",
            "u",
            "--jdbc-password",
            "p",
            "--jdbc-driver",
            "com.amazon.redshift.jdbc.Driver",
        ])
        assert args.jdbc_driver == "com.amazon.redshift.jdbc.Driver"

    def test_missing_required_args_fails(self):
        parser = self._make_parser()
        with pytest.raises(SystemExit):
            parser.parse_args([])

    def test_missing_jdbc_url_fails(self):
        parser = self._make_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["--jdbc-user", "u", "--jdbc-password", "p"])

    def test_jdbc_url_construction_pattern(self):
        """Verify the URL pattern matches the PostgreSQL JDBC format."""
        parser = self._make_parser()
        args = parser.parse_args([
            "--jdbc-url",
            "jdbc:postgresql://postgres:5432/w3c_warehouse",
            "--jdbc-user",
            "airflow",
            "--jdbc-password",
            "s3cret",
        ])
        assert args.jdbc_url.startswith("jdbc:postgresql://")
        assert "postgres" in args.jdbc_url
        assert "5432" in args.jdbc_url
        assert "w3c_warehouse" in args.jdbc_url


# ═══════════════════════════════════════════════════════════════════════════
#  3. Schema Mapping — apply_type_casts (requires PySpark)
# ═══════════════════════════════════════════════════════════════════════════


class TestApplyTypeCasts:
    """Verify ``apply_type_casts()`` transforms Delta types to PG-compatible types.

    These tests require PySpark (``pyspark.sql.SparkSession``) and are
    skipped if PySpark is not installed.  They use the ``spark`` fixture
    from ``conftest.py``.
    """

    def test_is_crawler_string_to_boolean_true(self, spark):
        _requires_pyspark()
        from pyspark.sql import Row
        from pyspark.sql.types import StringType, StructField, StructType

        from airflow.spark.jobs.export_warehouse import apply_type_casts

        schema = StructType([StructField("is_crawler", StringType(), True)])
        df = spark.createDataFrame([Row(is_crawler="true")], schema=schema)
        result = apply_type_casts(df)
        row = result.collect()[0]
        assert row.is_crawler is True

    def test_is_crawler_string_to_boolean_false(self, spark):
        _requires_pyspark()
        from pyspark.sql import Row
        from pyspark.sql.types import StringType, StructField, StructType

        from airflow.spark.jobs.export_warehouse import apply_type_casts

        schema = StructType([StructField("is_crawler", StringType(), True)])
        df = spark.createDataFrame([Row(is_crawler="false")], schema=schema)
        result = apply_type_casts(df)
        row = result.collect()[0]
        assert row.is_crawler is False

    def test_is_crawler_whitespace_insensitive(self, spark):
        _requires_pyspark()
        from pyspark.sql import Row
        from pyspark.sql.types import StringType, StructField, StructType

        from airflow.spark.jobs.export_warehouse import apply_type_casts

        schema = StructType([StructField("is_crawler", StringType(), True)])
        df = spark.createDataFrame([Row(is_crawler="  TRUE  ")], schema=schema)
        result = apply_type_casts(df)
        row = result.collect()[0]
        assert row.is_crawler is True

    def test_is_crawler_non_true_is_false(self, spark):
        _requires_pyspark()
        from pyspark.sql import Row
        from pyspark.sql.types import StringType, StructField, StructType

        from airflow.spark.jobs.export_warehouse import apply_type_casts

        schema = StructType([StructField("is_crawler", StringType(), True)])
        df = spark.createDataFrame([Row(is_crawler="unknown")], schema=schema)
        result = apply_type_casts(df)
        row = result.collect()[0]
        assert row.is_crawler is False

    def test_latitude_cast_to_double(self, spark):
        _requires_pyspark()
        from pyspark.sql import Row
        from pyspark.sql.types import StringType, StructField, StructType

        from airflow.spark.jobs.export_warehouse import apply_type_casts

        schema = StructType([StructField("latitude", StringType(), True)])
        df = spark.createDataFrame([Row(latitude="51.5074")], schema=schema)
        result = apply_type_casts(df)
        row = result.collect()[0]
        assert isinstance(row.latitude, float)
        assert abs(row.latitude - 51.5074) < 0.0001

    def test_longitude_cast_to_double(self, spark):
        _requires_pyspark()
        from pyspark.sql import Row
        from pyspark.sql.types import StringType, StructField, StructType

        from airflow.spark.jobs.export_warehouse import apply_type_casts

        schema = StructType([StructField("longitude", StringType(), True)])
        df = spark.createDataFrame([Row(longitude="-0.1276")], schema=schema)
        result = apply_type_casts(df)
        row = result.collect()[0]
        assert isinstance(row.longitude, float)
        assert abs(row.longitude - (-0.1276)) < 0.0001

    def test_bytes_sent_cast_to_bigint(self, spark):
        _requires_pyspark()
        from pyspark.sql import Row
        from pyspark.sql.types import LongType, StructField, StructType

        from airflow.spark.jobs.export_warehouse import apply_type_casts

        schema = StructType([StructField("bytes_sent", LongType(), True)])
        df = spark.createDataFrame([Row(bytes_sent=65535)], schema=schema)
        result = apply_type_casts(df)
        row = result.collect()[0]
        assert isinstance(row.bytes_sent, int)
        assert row.bytes_sent == 65535

    def test_bytes_recv_cast_to_bigint(self, spark):
        _requires_pyspark()
        from pyspark.sql import Row
        from pyspark.sql.types import LongType, StructField, StructType

        from airflow.spark.jobs.export_warehouse import apply_type_casts

        schema = StructType([StructField("bytes_recv", LongType(), True)])
        df = spark.createDataFrame([Row(bytes_recv=1024)], schema=schema)
        result = apply_type_casts(df)
        row = result.collect()[0]
        assert isinstance(row.bytes_recv, int)
        assert row.bytes_recv == 1024

    def test_time_taken_cast_to_bigint(self, spark):
        _requires_pyspark()
        from pyspark.sql import Row
        from pyspark.sql.types import LongType, StructField, StructType

        from airflow.spark.jobs.export_warehouse import apply_type_casts

        schema = StructType([StructField("time_taken", LongType(), True)])
        df = spark.createDataFrame([Row(time_taken=5000)], schema=schema)
        result = apply_type_casts(df)
        row = result.collect()[0]
        assert isinstance(row.time_taken, int)
        assert row.time_taken == 5000

    def test_apply_type_casts_all_transforms_together(self, spark):
        """Verify all casts work together on a row with all relevant columns."""
        _requires_pyspark()
        from pyspark.sql import Row
        from pyspark.sql.types import LongType, StringType, StructField, StructType

        from airflow.spark.jobs.export_warehouse import apply_type_casts

        schema = StructType([
            StructField("is_crawler", StringType(), True),
            StructField("latitude", StringType(), True),
            StructField("longitude", StringType(), True),
            StructField("bytes_sent", LongType(), True),
            StructField("bytes_recv", LongType(), True),
            StructField("time_taken", LongType(), True),
        ])
        df = spark.createDataFrame(
            [
                Row(
                    is_crawler="true",
                    latitude="40.7128",
                    longitude="-74.0060",
                    bytes_sent=1000,
                    bytes_recv=200,
                    time_taken=150,
                )
            ],
            schema=schema,
        )
        result = apply_type_casts(df).collect()[0]
        assert result.is_crawler is True
        assert isinstance(result.latitude, float)
        assert isinstance(result.longitude, float)
        assert isinstance(result.bytes_sent, int)
        assert isinstance(result.bytes_recv, int)
        assert isinstance(result.time_taken, int)

    def test_other_columns_preserved(self, spark):
        """Verify columns not in the cast list pass through unchanged."""
        _requires_pyspark()
        from pyspark.sql import Row
        from pyspark.sql.types import IntegerType, StringType, StructField, StructType

        from airflow.spark.jobs.export_warehouse import apply_type_casts

        schema = StructType([
            StructField("status", IntegerType(), True),
            StructField("is_crawler", StringType(), True),
        ])
        df = spark.createDataFrame([Row(status=200, is_crawler="false")], schema=schema)
        result = apply_type_casts(df).collect()[0]
        assert result.status == 200
        assert result.is_crawler is False

    def test_none_is_crawler_becomes_false(self, spark):
        """Null/is_crawler should become False (when not 'true')."""
        _requires_pyspark()
        from pyspark.sql import Row
        from pyspark.sql.types import StringType, StructField, StructType

        from airflow.spark.jobs.export_warehouse import apply_type_casts

        schema = StructType([StructField("is_crawler", StringType(), True)])
        df = spark.createDataFrame([Row(is_crawler=None)], schema=schema)
        result = apply_type_casts(df)
        row = result.collect()[0]
        assert row.is_crawler is False

    def test_none_latitude_stays_null(self, spark):
        """Null/latitude should remain null after casting to double."""
        _requires_pyspark()
        from pyspark.sql import Row
        from pyspark.sql.types import StringType, StructField, StructType

        from airflow.spark.jobs.export_warehouse import apply_type_casts

        schema = StructType([StructField("latitude", StringType(), True)])
        df = spark.createDataFrame([Row(latitude=None)], schema=schema)
        result = apply_type_casts(df)
        row = result.collect()[0]
        assert row.latitude is None


# ═══════════════════════════════════════════════════════════════════════════
#  4. Idempotency Tracking — get_loaded_source_files (requires PySpark)
# ═══════════════════════════════════════════════════════════════════════════


class TestGetLoadedSourceFiles:
    """Verify the tracking table query logic."""

    def test_returns_set_of_source_files(self, spark):
        """When the tracking table exists and has rows, return a set of values."""
        _requires_pyspark()
        from pyspark.sql import Row

        from airflow.spark.jobs.export_warehouse import get_loaded_source_files

        mock_reader = MagicMock()
        mock_reader.format.return_value = mock_reader
        mock_reader.option.return_value = mock_reader
        mock_reader.options.return_value = mock_reader

        expected_files = {"u_ex091024.log", "u_ex100718.log"}
        mock_df = spark.createDataFrame(
            [Row(source_file=f) for f in expected_files],
            schema="source_file string",
        )
        mock_reader.load.return_value = mock_df

        with patch.object(type(spark), "read", new_callable=PropertyMock, return_value=mock_reader):
            result = get_loaded_source_files(spark, "jdbc:postgresql://host:5432/db", {"user": "u", "password": "p"})

        assert result == expected_files

    def test_returns_empty_set_when_table_missing(self, spark):
        """When the tracking table does not exist, return an empty set."""
        _requires_pyspark()
        from airflow.spark.jobs.export_warehouse import get_loaded_source_files

        mock_reader = MagicMock()
        mock_reader.format.return_value = mock_reader
        mock_reader.option.return_value = mock_reader
        mock_reader.options.return_value = mock_reader
        mock_reader.load.side_effect = Exception("Table not found")

        with patch.object(type(spark), "read", new_callable=PropertyMock, return_value=mock_reader):
            result = get_loaded_source_files(spark, "jdbc:postgresql://host:5432/db", {"user": "u", "password": "p"})

        assert result == set()

    def test_correct_jdbc_options_passed(self, spark):
        """Verify the function calls JDBC reader with correct dbtable and props."""
        _requires_pyspark()
        from airflow.spark.jobs.export_warehouse import TRACKING_TABLE, get_loaded_source_files

        mock_reader = MagicMock()
        mock_reader.format.return_value = mock_reader
        mock_reader.option.return_value = mock_reader
        mock_reader.options.return_value = mock_reader

        empty_df = spark.createDataFrame([], schema="source_file string")
        mock_reader.load.return_value = empty_df

        jdbc_url = "jdbc:postgresql://pg:5432/w3c"
        jdbc_props = {"user": "airflow", "password": "pass", "driver": "org.postgresql.Driver"}

        with patch.object(type(spark), "read", new_callable=PropertyMock, return_value=mock_reader):
            get_loaded_source_files(spark, jdbc_url, jdbc_props)

        mock_reader.format.assert_called_with("jdbc")
        mock_reader.option.assert_any_call("url", jdbc_url)
        mock_reader.option.assert_any_call("dbtable", TRACKING_TABLE)
        mock_reader.options.assert_called_with(**jdbc_props)
        mock_reader.load.assert_called_once()


# ═══════════════════════════════════════════════════════════════════════════
#  5. Idempotency Tracking — insert_tracking_records (requires PySpark)
# ═══════════════════════════════════════════════════════════════════════════


class TestInsertTrackingRecords:
    """Verify tracking record insertion logic."""

    def test_inserts_new_source_files(self, spark):
        """When new source files are provided, a DataFrame is created and appended."""
        _requires_pyspark()
        from airflow.spark.jobs.export_warehouse import TRACKING_TABLE, insert_tracking_records

        new_files = {"u_ex091024.log", "u_ex100718.log", "u_ex110101.log"}
        mock_writer = MagicMock()
        mock_writer.format.return_value = mock_writer
        mock_writer.mode.return_value = mock_writer
        mock_writer.option.return_value = mock_writer
        mock_writer.options.return_value = mock_writer

        mock_df = MagicMock()
        mock_df.write = mock_writer

        with patch.object(spark, "createDataFrame", return_value=mock_df) as mock_create:
            insert_tracking_records(spark, "jdbc:postgresql://pg:5432/w3c", {"user": "u", "password": "p"}, new_files)

            mock_create.assert_called_once()
            mock_writer.format.assert_called_with("jdbc")
            mock_writer.mode.assert_called_with("append")
            mock_writer.option.assert_any_call("dbtable", TRACKING_TABLE)
            mock_writer.save.assert_called_once()

    def test_noop_when_empty_set(self, spark):
        """When new_source_files is empty, nothing is written."""
        _requires_pyspark()
        from airflow.spark.jobs.export_warehouse import insert_tracking_records

        with patch.object(spark, "createDataFrame") as mock_create_df:
            insert_tracking_records(spark, "jdbc:postgresql://pg:5432/w3c", {"user": "u", "password": "p"}, set())
            mock_create_df.assert_not_called()


# ═══════════════════════════════════════════════════════════════════════════
#  6. Error Handling — tracking table NOT updated on JDBC failure
# ═══════════════════════════════════════════════════════════════════════════


class TestErrorHandling:
    """Verify the tracking table is NOT updated when the JDBC write fails."""

    def _mock_run_deps(self, spark, fail_on_write=False):
        """Helper to set up mocks for the ``run()`` function."""
        _requires_pyspark()
        # We patch at the module level to avoid importing until pyspark is confirmed
        from pyspark.sql import Row

        from airflow.spark.jobs.export_warehouse import run

        silver_df = spark.createDataFrame([
            Row(source_file="u_ex091024.log", is_crawler="true"),
            Row(source_file="u_ex100718.log", is_crawler="false"),
        ])

        patcher_read = patch.object(type(spark), "read", new_callable=PropertyMock)
        mock_read_prop = patcher_read.start()
        mock_reader = MagicMock()
        mock_reader.format.return_value = mock_reader
        mock_reader.load.return_value = silver_df
        mock_read_prop.return_value = mock_reader

        patcher_get = patch("airflow.spark.jobs.export_warehouse.get_loaded_source_files")
        mock_get = patcher_get.start()
        mock_get.return_value = set()

        patcher_cast = patch("airflow.spark.jobs.export_warehouse.apply_type_casts")
        mock_cast = patcher_cast.start()

        patcher_ddl = patch("airflow.spark.jobs.export_warehouse.execute_ddl")
        _mock_ddl = patcher_ddl.start()

        patcher_insert = patch("airflow.spark.jobs.export_warehouse.insert_tracking_records")
        mock_insert = patcher_insert.start()

        class Writer:
            def __init__(self, fail=False):
                self._fail = fail

            def format(self, *a, **kw):
                return self

            def mode(self, *a, **kw):
                return self

            def option(self, *a, **kw):
                return self

            def options(self, **kw):
                return self

            def save(self, **kw):
                if self._fail:
                    raise RuntimeError("Connection refused: PostgreSQL is down")

        cast_df = MagicMock()
        cast_df.write = Writer(fail=fail_on_write)
        mock_cast.return_value = cast_df

        def cleanup():
            patcher_read.stop()
            patcher_get.stop()
            patcher_cast.stop()
            patcher_ddl.stop()
            patcher_insert.stop()

        return run, mock_insert, cleanup

    def test_tracking_not_updated_on_write_failure(self, spark):
        """When the JDBC append write raises, insert_tracking_records is never called."""
        _requires_pyspark()
        run, mock_insert, cleanup = self._mock_run_deps(spark, fail_on_write=True)

        with patch("airflow.spark.jobs.export_warehouse.log") as mock_log:
            with pytest.raises(RuntimeError, match="Connection refused"):
                run(
                    spark,
                    delta_dir="/fake/delta",
                    jdbc_url="jdbc:postgresql://pg:5432/w3c",
                    jdbc_user="u",
                    jdbc_password="p",
                    jdbc_driver="org.postgresql.Driver",
                )

            mock_insert.assert_not_called()
            mock_log.error.assert_any_call(
                "JDBC write to %s failed: %s. Tracking table NOT updated.",
                ANY,
                ANY,
            )
        cleanup()

    def test_tracking_updated_on_success(self, spark):
        """When the JDBC write succeeds, insert_tracking_records IS called."""
        _requires_pyspark()
        run, mock_insert, cleanup = self._mock_run_deps(spark, fail_on_write=False)

        run(
            spark,
            delta_dir="/fake/delta",
            jdbc_url="jdbc:postgresql://pg:5432/w3c",
            jdbc_user="u",
            jdbc_password="p",
            jdbc_driver="org.postgresql.Driver",
        )

        mock_insert.assert_called_once()
        args, _ = mock_insert.call_args
        assert args[3] == {"u_ex091024.log", "u_ex100718.log"}
        cleanup()


# ═══════════════════════════════════════════════════════════════════════════
#  7. run() — Pipeline orchestration edge cases
# ═══════════════════════════════════════════════════════════════════════════


class TestRunEdgeCases:
    """Verify the main pipeline handles empty / missing data gracefully."""

    def test_empty_delta_table_returns_early(self, spark):
        """When the silver table has zero rows, run() should return without writing."""
        _requires_pyspark()
        from airflow.spark.jobs.export_warehouse import run

        empty_df = spark.createDataFrame([], schema="source_file string")

        mock_reader = MagicMock()
        mock_reader.format.return_value = mock_reader
        mock_reader.load.return_value = empty_df

        with patch.object(type(spark), "read", new_callable=PropertyMock, return_value=mock_reader):
            with patch("airflow.spark.jobs.export_warehouse.log") as mock_log:
                result = run(
                    spark,
                    delta_dir="/fake/delta",
                    jdbc_url="jdbc:postgresql://pg:5432/w3c",
                    jdbc_user="u",
                    jdbc_password="p",
                    jdbc_driver="org.postgresql.Driver",
                )

                assert result is None
                mock_log.warning.assert_called_with("Silver table is empty. Nothing to export.")

    def test_all_files_already_tracked(self, spark):
        """When all source files are already in the tracking table, skip export."""
        _requires_pyspark()
        from pyspark.sql import Row

        from airflow.spark.jobs.export_warehouse import run

        df = spark.createDataFrame([
            Row(source_file="u_ex091024.log"),
        ])

        mock_reader = MagicMock()
        mock_reader.format.return_value = mock_reader
        mock_reader.load.return_value = df

        with patch.object(type(spark), "read", new_callable=PropertyMock, return_value=mock_reader):
            with patch(
                "airflow.spark.jobs.export_warehouse.get_loaded_source_files",
                return_value={"u_ex091024.log"},
            ):
                with patch("airflow.spark.jobs.export_warehouse.log") as mock_log:
                    run(
                        spark,
                        delta_dir="/fake/delta",
                        jdbc_url="jdbc:postgresql://pg:5432/w3c",
                        jdbc_user="u",
                        jdbc_password="p",
                        jdbc_driver="org.postgresql.Driver",
                    )

                    mock_log.info.assert_any_call("No new source files to export. All files already in PostgreSQL.")

    def test_cannot_read_delta_table(self, spark):
        """When Delta table cannot be read, the error is logged and function returns."""
        _requires_pyspark()
        from airflow.spark.jobs.export_warehouse import run

        mock_reader = MagicMock()
        mock_reader.format.return_value = mock_reader
        mock_reader.load.side_effect = Exception("Delta log not found")

        with patch.object(type(spark), "read", new_callable=PropertyMock, return_value=mock_reader):
            with patch("airflow.spark.jobs.export_warehouse.log") as mock_log:
                result = run(
                    spark,
                    delta_dir="/fake/delta",
                    jdbc_url="jdbc:postgresql://pg:5432/w3c",
                    jdbc_user="u",
                    jdbc_password="p",
                    jdbc_driver="org.postgresql.Driver",
                )

                assert result is None
                mock_log.error.assert_called_once()
                assert "Cannot read Silver Delta table" in mock_log.error.call_args[0][0]


# ═══════════════════════════════════════════════════════════════════════════
#  8. execute_ddl (requires PySpark)
# ═══════════════════════════════════════════════════════════════════════════


class TestExecuteDDL:
    """Verify DDL execution uses the JDBC connection correctly."""

    def test_execute_ddl_uses_jdbc_connection(self, spark):
        """Verify execute_ddl obtains a psycopg2 connection and executes the statement."""
        _requires_pyspark()
        from airflow.spark.jobs.export_warehouse import execute_ddl

        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value = mock_cur

        with patch("psycopg2.connect", return_value=mock_conn):
            execute_ddl(
                spark,
                "jdbc:postgresql://postgres:5432/w3c_warehouse",
                {"user": "u", "password": "p", "driver": "org.postgresql.Driver"},
                "CREATE TABLE IF NOT EXISTS test (id INT)",
                "test_table",
            )

        mock_conn.cursor.assert_called_once()
        mock_cur.execute.assert_called_once_with("CREATE TABLE IF NOT EXISTS test (id INT)")
        mock_conn.commit.assert_called_once()
        mock_cur.close.assert_called_once()
        mock_conn.close.assert_called_once()

    def test_execute_ddl_logs_and_raises_on_failure(self, spark):
        """When the psycopg2 connection fails, the error is logged and re-raised."""
        _requires_pyspark()
        from airflow.spark.jobs.export_warehouse import execute_ddl

        with patch("psycopg2.connect", side_effect=RuntimeError("No database")):
            with patch("airflow.spark.jobs.export_warehouse.log") as mock_log:
                with pytest.raises(RuntimeError):
                    execute_ddl(
                        spark,
                        "jdbc:postgresql://postgres:5432/w3c_warehouse",
                        {"user": "u", "password": "p", "driver": "org.postgresql.Driver"},
                        "CREATE TABLE test (id INT)",
                        "test_table",
                    )

                mock_log.error.assert_called_once()
                assert "Failed to execute DDL" in mock_log.error.call_args[0][0]
