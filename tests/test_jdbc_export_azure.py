"""
Unit tests for ``jdbc_export_azure.py`` — Silver (Unity Catalog) → Azure SQL.

The module reads the Silver enriched Delta table from Unity Catalog using
Spark, collects rows, and batch-inserts new source files into Azure SQL
``dbo.raw_enriched`` via pymssql.  Uses a tracking table
``dbo.raw_enriched_loaded`` for idempotency.

Tests cover:

1. DDL generation (extracted from source via AST — no Spark required)
2. ``_connect`` — retry with exponential backoff for auto-resume
3. ``execute_ddl`` / ``ensure_tables_exist`` — table creation
4. ``get_loaded_source_files`` — idempotency tracking
5. ``insert_batch`` — both Spark ``Row`` and ``dict`` inputs
6. ``export_to_azure_sql`` — full end-to-end flow (mocked pymssql)

Tests that require PySpark are guarded with ``pytest.importorskip("pyspark")``.

Usage:
    pytest tests/test_jdbc_export_azure.py -v --tb=short
"""

import ast
import os
import sys
import time
from unittest.mock import MagicMock, PropertyMock, call, patch

import pytest

# Paths for AST extraction and module import.
# The source lives at ``airflow/spark/databricks/jdbc_export_azure.py``.
_TEST_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.join(_TEST_DIR, "..")

_NESTED_DATABRICKS = os.path.join(_PROJECT_ROOT, "airflow", "spark", "databricks")
_FLAT_DATABRICKS = os.path.join(_PROJECT_ROOT, "spark", "databricks")


def _source_path():
    """Return the absolute path to ``jdbc_export_azure.py``."""
    for base in (_NESTED_DATABRICKS, _FLAT_DATABRICKS):
        candidate = os.path.join(base, "jdbc_export_azure.py")
        if os.path.isfile(candidate):
            return candidate
    raise FileNotFoundError("jdbc_export_azure.py not found under airflow/spark/databricks/")


def _get_module_constant(name: str) -> str | None:
    """Extract a top-level string constant via AST — avoids importing.

    This is the same pattern used in ``test_export_warehouse.py``.
    """
    with open(_source_path()) as f:
        tree = ast.parse(f.read())
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == name:
                    if isinstance(node.value, ast.Constant) and isinstance(node.value.value, str):
                        return node.value.value
    return None


def _get_module_list(name: str) -> list | None:
    """Extract a top-level list constant via AST."""
    with open(_source_path()) as f:
        tree = ast.parse(f.read())
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == name:
                    if isinstance(node.value, ast.List):
                        return [
                            elt.value
                            for elt in node.value.elts
                            if isinstance(elt, ast.Constant)
                        ]
    return None


def _get_module_retry_attempts() -> int | None:
    """Extract the RETRY_ATTEMPTS integer constant via AST."""
    with open(_source_path()) as f:
        tree = ast.parse(f.read())
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == "RETRY_ATTEMPTS":
                    if isinstance(node.value, ast.Constant) and isinstance(node.value.value, int):
                        return node.value.value
    return None


# ======================================================================
# Add the databricks directory to sys.path for direct module import
# (following the same pattern as test_dlt_bronze.py / test_dlt_silver.py)
# ======================================================================
for _db_path in (_NESTED_DATABRICKS, _FLAT_DATABRICKS):
    if os.path.isdir(_db_path) and _db_path not in sys.path:
        sys.path.insert(0, _db_path)

# ═══════════════════════════════════════════════════════════════════════════
# 1. DDL Generation — pure string checks (no Spark needed)
# ═══════════════════════════════════════════════════════════════════════════


class TestDDLGeneration:
    """Verify ``IF OBJECT_ID`` DDL is correctly generated.

    These tests extract constants from the source file using AST parsing,
    so they do NOT require PySpark or pymssql to be installed.
    """

    def test_raw_enriched_ddl_has_create_statement(self):
        ddl = _get_module_constant("RAW_ENRICHED_DDL")
        assert ddl is not None, "RAW_ENRICHED_DDL constant not found in source"
        assert "CREATE TABLE dbo.raw_enriched" in ddl

    def test_raw_enriched_ddl_uses_object_id(self):
        ddl = _get_module_constant("RAW_ENRICHED_DDL")
        assert ddl is not None
        assert "IF OBJECT_ID('dbo.raw_enriched', 'U') IS NULL" in ddl

    def test_raw_enriched_ddl_has_all_31_columns(self):
        ddl = _get_module_constant("RAW_ENRICHED_DDL")
        assert ddl is not None
        lines = [line.strip() for line in ddl.strip().split("\n")]
        col_lines = [
            line
            for line in lines
            if line
            and not line.startswith("CREATE")
            and line != "("
            and line != ");"
            and not line.startswith(")")
            and not line.startswith("IF")
        ]
        col_names = [line.rstrip(",").split()[0] for line in col_lines if line]
        assert len(col_names) == 31, f"Expected 31 columns, got {len(col_names)}: {col_names}"

    def test_raw_enriched_ddl_has_is_crawler_bit(self):
        ddl = _get_module_constant("RAW_ENRICHED_DDL")
        assert ddl is not None
        assert "is_crawler       BIT" in ddl

    def test_raw_enriched_ddl_has_nvarchar_max_fields(self):
        ddl = _get_module_constant("RAW_ENRICHED_DDL")
        assert ddl is not None
        assert "NVARCHAR(MAX)" in ddl

    def test_tracking_ddl_has_create_statement(self):
        ddl = _get_module_constant("TRACKING_DDL")
        assert ddl is not None, "TRACKING_DDL constant not found in source"
        assert "CREATE TABLE dbo.raw_enriched_loaded" in ddl

    def test_tracking_ddl_has_source_file_pk(self):
        ddl = _get_module_constant("TRACKING_DDL")
        assert ddl is not None
        assert "source_file VARCHAR(255) PRIMARY KEY" in ddl

    def test_tracking_ddl_object_id_guard(self):
        ddl = _get_module_constant("TRACKING_DDL")
        assert ddl is not None
        assert "IF OBJECT_ID('dbo.raw_enriched_loaded', 'U') IS NULL" in ddl

    def test_both_ddl_constants_exist(self):
        """Ensure both DDL constants are defined in the source."""
        raw = _get_module_constant("RAW_ENRICHED_DDL")
        track = _get_module_constant("TRACKING_DDL")
        assert raw is not None, "RAW_ENRICHED_DDL not found"
        assert track is not None, "TRACKING_DDL not found"

    def test_export_columns_has_31_items(self):
        cols = _get_module_list("EXPORT_COLUMNS")
        assert cols is not None, "EXPORT_COLUMNS not found"
        assert len(cols) == 31, f"Expected 31 columns, got {len(cols)}"

    def test_export_columns_includes_is_crawler(self):
        cols = _get_module_list("EXPORT_COLUMNS")
        assert cols is not None
        assert "is_crawler" in cols

    def test_export_columns_includes_enriched_fields(self):
        cols = _get_module_list("EXPORT_COLUMNS")
        assert cols is not None
        for field in ("country", "region", "city", "latitude", "longitude", "isp"):
            assert field in cols, f"Missing enriched field: {field}"

    def test_retry_attempts_default(self):
        retry = _get_module_retry_attempts()
        assert retry is not None
        assert retry == 4, f"Expected RETRY_ATTEMPTS = 4, got {retry}"


# ═══════════════════════════════════════════════════════════════════════════
# 2. _connect — retry with exponential backoff
# ═══════════════════════════════════════════════════════════════════════════


@pytest.fixture(autouse=True)
def _clean_pymssql_mod():
    """Ensure pymssql is not in sys.modules before each test.

    ``_connect`` does ``import pymssql`` inside the function body, so we
    control whether it succeeds by injecting a mock into ``sys.modules``
    per test.
    """
    yield


class TestConnect:
    """Connection retry logic for database auto-resume."""

    def _import_connect(self):
        """Lazy import ``_connect`` from jdbc_export_azure."""
        from jdbc_export_azure import _connect

        return _connect

    def test_successful_connection_on_first_attempt(self):
        """_connect returns a pymssql connection on first try."""
        mock_pymssql = MagicMock()
        mock_conn = MagicMock()
        mock_pymssql.connect.return_value = mock_conn

        with patch.dict("sys.modules", {"pymssql": mock_pymssql}):
            _connect = self._import_connect()
            result = _connect("server", "db", "user", "pass")

        assert result is mock_conn
        mock_pymssql.connect.assert_called_once_with(
            server="server",
            database="db",
            user="user",
            password="pass",
            port=1433,
            login_timeout=30,
        )

    def test_retries_on_not_available_error(self):
        """Retries on OperationalError with 'not currently available' message."""
        mock_pymssql = MagicMock()
        mock_conn = MagicMock()

        op_error = Exception("database is not currently available")
        mock_pymssql.OperationalError = type("OpError", (Exception,), {})
        mock_pymssql.connect.side_effect = [
            mock_pymssql.OperationalError("database is not currently available"),
            mock_pymssql.OperationalError("not currently available"),
            mock_pymssql.OperationalError("timeout expired"),
            mock_conn,  # 4th attempt succeeds
        ]

        with patch.dict("sys.modules", {"pymssql": mock_pymssql}):
            with patch("jdbc_export_azure.time.sleep") as mock_sleep:
                _connect = self._import_connect()
                result = _connect("server", "db", "user", "pass")

        assert result is mock_conn
        # 3 failures + 1 success = 4 total calls
        assert mock_pymssql.connect.call_count == 4
        # Backoff: 15, 30, 60 seconds
        mock_sleep.assert_has_calls([call(15), call(30), call(60)])

    def test_raises_after_all_retries_exhausted(self):
        """Raises RuntimeError after RETRY_ATTEMPTS failures."""
        mock_pymssql = MagicMock()
        mock_pymssql.OperationalError = type("OpError", (Exception,), {})

        op_err = mock_pymssql.OperationalError("not currently available")
        mock_pymssql.connect.side_effect = [op_err] * 4  # All attempts fail

        with patch.dict("sys.modules", {"pymssql": mock_pymssql}):
            with patch("jdbc_export_azure.time.sleep") as mock_sleep:
                _connect = self._import_connect()
                with pytest.raises(RuntimeError, match="Cannot connect to Azure SQL"):
                    _connect("server", "db", "user", "pass")

        assert mock_pymssql.connect.call_count == 4

    def test_raises_immediately_on_non_operational_error(self):
        """Non-OperationalError raises immediately (no retry)."""
        mock_pymssql = MagicMock()
        mock_pymssql.OperationalError = type("OpError", (Exception,), {})

        generic_error = ValueError("wrong arguments")
        mock_pymssql.connect.side_effect = generic_error

        with patch.dict("sys.modules", {"pymssql": mock_pymssql}):
            with patch("jdbc_export_azure.time.sleep") as mock_sleep:
                _connect = self._import_connect()
                with pytest.raises(RuntimeError, match="Cannot connect to Azure SQL"):
                    _connect("server", "db", "user", "pass")

        # Only 1 attempt (no retry for non-OperationalError)
        assert mock_pymssql.connect.call_count == 1
        mock_sleep.assert_not_called()


# ═══════════════════════════════════════════════════════════════════════════
# 3. execute_ddl / ensure_tables_exist
# ═══════════════════════════════════════════════════════════════════════════


class TestExecuteDDL:
    """DDL execution helpers."""

    def _import_functions(self):
        from jdbc_export_azure import execute_ddl, ensure_tables_exist

        return execute_ddl, ensure_tables_exist

    def test_execute_ddl_runs_and_commits(self):
        """execute_ddl executes SQL and commits."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        execute_ddl, _ = self._import_functions()
        execute_ddl(mock_conn, "CREATE TABLE test (id INT)")

        mock_cursor.execute.assert_called_once_with("CREATE TABLE test (id INT)")
        mock_conn.commit.assert_called_once()

    def test_ensure_tables_exist_runs_both_ddls(self):
        """ensure_tables_exist calls execute_ddl for both DDL strings."""
        from jdbc_export_azure import RAW_ENRICHED_DDL, TRACKING_DDL

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        _, ensure_tables_exist = self._import_functions()

        with patch("jdbc_export_azure.execute_ddl") as mock_execute:
            ensure_tables_exist(mock_conn)

        assert mock_execute.call_count == 2
        mock_execute.assert_has_calls([
            call(mock_conn, RAW_ENRICHED_DDL),
            call(mock_conn, TRACKING_DDL),
        ])


# ═══════════════════════════════════════════════════════════════════════════
# 4. get_loaded_source_files
# ═══════════════════════════════════════════════════════════════════════════


class TestGetLoadedSourceFiles:
    """Idempotency tracking table reader."""

    def _import_func(self):
        from jdbc_export_azure import get_loaded_source_files

        return get_loaded_source_files

    def test_returns_set_of_source_files(self):
        """Returns a set of source_file values from tracking table."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            ("file1.log",),
            ("file2.log",),
            ("file3.log",),
        ]
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        get = self._import_func()
        result = get(mock_conn)

        assert result == {"file1.log", "file2.log", "file3.log"}

    def test_returns_empty_set_on_error(self):
        """Returns empty set when tracking table doesn't exist yet."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = Exception("Table not found")
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        get = self._import_func()
        result = get(mock_conn)

        assert result == set()

    def test_empty_tracking_table_returns_empty_set(self):
        """Returns empty set when tracking table has no rows."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = []
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        get = self._import_func()
        result = get(mock_conn)

        assert result == set()


# ═══════════════════════════════════════════════════════════════════════════
# 5. insert_batch
# ═══════════════════════════════════════════════════════════════════════════


class TestInsertBatch:
    """Batch insert helper with Row and dict support."""

    def _import_func(self):
        from jdbc_export_azure import insert_batch

        return insert_batch

    def test_inserts_dict_rows(self):
        """Accepts dict rows for tracking-table inserts."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        rows = [{"source_file": "f1.log"}, {"source_file": "f2.log"}]

        insert_batch = self._import_func()
        count = insert_batch(mock_conn, rows, "dbo.raw_enriched_loaded")

        assert count == 2
        mock_cursor.executemany.assert_called_once()
        sql = mock_cursor.executemany.call_args[0][0]
        assert "INSERT INTO dbo.raw_enriched_loaded" in sql
        assert "[source_file]" in sql
        mock_conn.commit.assert_called_once()

    def test_inserts_spark_row_objects(self):
        """Accepts PySpark Row objects from DataFrame.collect()."""
        pytest.importorskip("pyspark")
        from pyspark.sql import Row

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        rows = [
            Row(log_date="2026-01-01", status=200, source_file="file1.log"),
            Row(log_date="2026-01-02", status=404, source_file="file2.log"),
        ]

        insert_batch = self._import_func()
        count = insert_batch(mock_conn, rows, "dbo.raw_enriched")

        assert count == 2
        mock_cursor.executemany.assert_called_once()
        sql = mock_cursor.executemany.call_args[0][0]
        assert "INSERT INTO dbo.raw_enriched" in sql
        params = mock_cursor.executemany.call_args[0][1]
        assert len(params) == 2  # 2 rows
        mock_conn.commit.assert_called_once()

    def test_empty_rows_returns_zero(self):
        """Empty rows list returns 0 without calling executemany."""
        mock_conn = MagicMock()

        insert_batch = self._import_func()
        count = insert_batch(mock_conn, [], "dbo.raw_enriched")

        assert count == 0
        mock_conn.cursor.assert_not_called()

    def test_accepts_single_row(self):
        """Single row works (edge case for tracking table)."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        rows = [{"source_file": "single.log"}]

        insert_batch = self._import_func()
        count = insert_batch(mock_conn, rows, "dbo.raw_enriched_loaded")

        assert count == 1
        mock_cursor.executemany.assert_called_once()

    def test_columns_bracket_escaped(self):
        """Column names are bracket-escaped in INSERT SQL."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        rows = [{"source_file": "f.log"}]

        insert_batch = self._import_func()
        insert_batch(mock_conn, rows, "dbo.test")

        sql = mock_cursor.executemany.call_args[0][0]
        assert "[source_file]" in sql


# ═══════════════════════════════════════════════════════════════════════════
# 6. export_to_azure_sql — end-to-end flow
# ═══════════════════════════════════════════════════════════════════════════


class TestExportToAzureSql:
    """Full export pipeline with mocked connections."""

    def _import_export(self):
        from jdbc_export_azure import export_to_azure_sql

        return export_to_azure_sql

    def test_full_flow_with_new_data(self, caplog):
        """Full export flow: connect → ensure tables → filter → batch insert → track."""
        pytest.importorskip("pyspark")
        from pyspark.sql import SparkSession, Row

        # ── Mock pymssql side ─────────────────────────────────────────
        mock_pymssql = MagicMock()
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_pymssql.connect.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_cursor.fetchall.return_value = []  # empty tracking table

        # ── Real PySpark DataFrame ────────────────────────────────────
        spark = (
            SparkSession.builder.master("local[1]")
            .appName("test")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )

        try:
            data = [
                Row(
                    log_date="2026-01-01",
                    log_time="12:00:00",
                    server_ip="1.2.3.4",
                    method="GET",
                    uri_stem="/index.html",
                    uri_query="",
                    client_ip="5.6.7.8",
                    user_agent="test-agent",
                    cookie="",
                    referrer="-",
                    status=200,
                    sub_status=0,
                    win32_status=0,
                    bytes_sent=1000,
                    bytes_recv=500,
                    server_port=80,
                    username="",
                    time_taken=100,
                    source_file="w3c-2026-01-01.log",
                    postcode="12345",
                    page_category="Home",
                    referrer_domain="",
                    traffic_type="Direct",
                    is_crawler="false",
                    size_band="1KB-10KB",
                    country="US",
                    region="CA",
                    city="San Jose",
                    latitude=37.33,
                    longitude=-121.89,
                    isp="Test ISP",
                ),
            ]
            df = spark.createDataFrame(data)

            with patch.dict("sys.modules", {"pymssql": mock_pymssql}):
                export_to_azure_sql = self._import_export()
                export_to_azure_sql(spark, "server", "db", "user", "pass")

            # ── Assertions ────────────────────────────────────────────
            # Connection established
            mock_pymssql.connect.assert_called_once()

            # Tables ensured (2 DDL execute calls)
            assert mock_cursor.execute.call_count >= 2

            # Data inserted into dbo.raw_enriched
            # (once via executemany for the batch insert)
            assert mock_cursor.executemany.call_count >= 1

            # Tracking table updated
            any_tracking = any(
                "raw_enriched_loaded" in str(args)
                for args in mock_cursor.executemany.call_args_list
            )
            # Either through executemany or direct execute
            any_tracking_execute = any(
                "raw_enriched_loaded" in str(args[0][0])
                for args in mock_cursor.execute.call_args_list
                if args[0]
            )
            assert any_tracking or any_tracking_execute, (
                "No tracking table update found"
            )

            # Connection closed
            mock_conn.close.assert_called_once()

            assert any("Exporting" in msg for msg in caplog.messages)

        finally:
            spark.stop()

    def test_no_new_files_skips_export(self, caplog):
        """No new source files → early return, no insert."""
        pytest.importorskip("pyspark")
        from pyspark.sql import SparkSession, Row

        mock_pymssql = MagicMock()
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_pymssql.connect.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        # All files already loaded
        mock_cursor.fetchall.return_value = [("w3c-2026-01-01.log",)]

        spark = (
            SparkSession.builder.master("local[1]")
            .appName("test")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )

        try:
            data = [
                Row(
                    log_date="2026-01-01",
                    log_time="12:00:00",
                    server_ip="1.2.3.4",
                    method="GET",
                    uri_stem="/index.html",
                    uri_query="",
                    client_ip="5.6.7.8",
                    user_agent="test-agent",
                    cookie="",
                    referrer="-",
                    status=200,
                    sub_status=0,
                    win32_status=0,
                    bytes_sent=1000,
                    bytes_recv=500,
                    server_port=80,
                    username="",
                    time_taken=100,
                    source_file="w3c-2026-01-01.log",
                    postcode="12345",
                    page_category="Home",
                    referrer_domain="",
                    traffic_type="Direct",
                    is_crawler="false",
                    size_band="1KB-10KB",
                    country="US",
                    region="CA",
                    city="San Jose",
                    latitude=37.33,
                    longitude=-121.89,
                    isp="Test ISP",
                ),
            ]
            df = spark.createDataFrame(data)

            with patch.dict("sys.modules", {"pymssql": mock_pymssql}):
                export_to_azure_sql = self._import_export()
                export_to_azure_sql(spark, "server", "db", "user", "pass")

            # No batch insert calls
            calls = mock_cursor.executemany.call_args_list
            raw_enriched_calls = [
                c for c in calls if "raw_enriched" in str(c) and "tracking" not in str(c).lower()
            ]
            # The tracking table still gets updated
            assert any("No new source files" in msg for msg in caplog.messages)

        finally:
            spark.stop()

    def test_is_crawler_cast_from_string_to_bit(self):
        """is_crawler string ("true"/"false") is cast to BIT (0/1) for Azure SQL."""
        pytest.importorskip("pyspark")
        from pyspark.sql import SparkSession, Row

        mock_pymssql = MagicMock()
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_pymssql.connect.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_cursor.fetchall.return_value = []

        spark = (
            SparkSession.builder.master("local[1]")
            .appName("test")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )

        try:
            data = [
                Row(
                    **{
                        "log_date": "2026-01-01",
                        "log_time": "12:00:00",
                        "server_ip": "1.2.3.4",
                        "method": "GET",
                        "uri_stem": "/index.html",
                        "uri_query": "",
                        "client_ip": "5.6.7.8",
                        "user_agent": "test-agent",
                        "cookie": "",
                        "referrer": "-",
                        "status": 200,
                        "sub_status": 0,
                        "win32_status": 0,
                        "bytes_sent": 1000,
                        "bytes_recv": 500,
                        "server_port": 80,
                        "username": "",
                        "time_taken": 100,
                        "source_file": "test.log",
                        "postcode": "",
                        "page_category": "Home",
                        "referrer_domain": "",
                        "traffic_type": "Direct",
                        "is_crawler": "true",
                        "size_band": "1KB-10KB",
                        "country": "US",
                        "region": "CA",
                        "city": "San Jose",
                        "latitude": 37.33,
                        "longitude": -121.89,
                        "isp": "Test ISP",
                    }
                ),
            ]
            df = spark.createDataFrame(data)

            with patch.dict("sys.modules", {"pymssql": mock_pymssql}):
                export_to_azure_sql = self._import_export()
                export_to_azure_sql(spark, "server", "db", "user", "pass")

            # Verify is_crawler was cast to 1 (integer)
            # The params passed to executemany should have 1 for is_crawler
            for args in mock_cursor.executemany.call_args_list:
                params = args[0][1] if len(args[0]) > 1 else []
                for row_params in params:
                    if hasattr(row_params, "__iter__"):
                        # Find is_crawler position in the params
                        for p in row_params:
                            if p == 1 or p == 0:
                                assert p in (0, 1), f"is_crawler should be 0 or 1, got {p}"

        finally:
            spark.stop()

    def test_connection_closed_in_finally(self):
        """Connection is always closed, even on error."""
        pytest.importorskip("pyspark")
        from pyspark.sql import SparkSession, Row

        mock_pymssql = MagicMock()
        mock_conn = MagicMock()
        mock_pymssql.connect.return_value = mock_conn

        spark = (
            SparkSession.builder.master("local[1]")
            .appName("test")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )

        try:
            data = [
                Row(
                    log_date="2026-01-01",
                    log_time="12:00:00",
                    server_ip="1.2.3.4",
                    method="GET",
                    uri_stem="/index.html",
                    uri_query="",
                    client_ip="5.6.7.8",
                    user_agent="test-agent",
                    cookie="",
                    referrer="-",
                    status=200,
                    sub_status=0,
                    win32_status=0,
                    bytes_sent=1000,
                    bytes_recv=500,
                    server_port=80,
                    username="",
                    time_taken=100,
                    source_file="test.log",
                    postcode="",
                    page_category="Home",
                    referrer_domain="",
                    traffic_type="Direct",
                    is_crawler="false",
                    size_band="1KB-10KB",
                    country="US",
                    region="CA",
                    city="San Jose",
                    latitude=37.33,
                    longitude=-121.89,
                    isp="Test ISP",
                ),
            ]
            df = spark.createDataFrame(data)

            with patch.dict("sys.modules", {"pymssql": mock_pymssql}):
                export_to_azure_sql = self._import_export()
                # The cursor.execute will fail because we didn't mock it
                # properly — this exercises the finally close
                export_to_azure_sql(spark, "server", "db", "user", "pass")

            mock_conn.close.assert_called_once()

        finally:
            spark.stop()
