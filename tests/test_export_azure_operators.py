"""
Unit tests for Phase 8c/8d Azure SQL operators and ``dbt_common`` bootstrap.

Covers three modules that were added during Phase 8c/8d and have **no
pre-existing unit tests**:

1. ``plugins.operators.export_csv_azure`` — CSV export from Azure SQL to
   ``/opt/airflow/data/Star-Schema/`` for Power BI (with failure tracking).
2. ``plugins.operators.export_dbt_docs_azure`` — Download dbt documentation
   artifacts from Azure Blob Storage (gold container) to local Airflow.
3. ``dbt_common`` — Shared bootstrap utilities for Databricks dbt notebooks
   (``_write_profiles_yml``, ``run_dbt_command``, ``cleanup``).

All tests are hermetic (mock ``pyodbc``, ``pandas``, ``azure.storage.blob``,
``subprocess``, ``os``, ``shutil``) so they run in CI without any Azure,
Databricks, or ODBC driver.

Usage:
    pytest tests/test_export_azure_operators.py -v --tb=short
"""

from __future__ import annotations

import builtins
import os
import sys
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import MagicMock, mock_open, patch

import pytest

# ── Project-relative paths ─────────────────────────────────────────────
_TEST_DIR = Path(__file__).resolve().parent
_PROJECT_ROOT = _TEST_DIR.parent
_AIRFLOW_DIR = _PROJECT_ROOT / "airflow"
_DATABRICKS_DIR = _AIRFLOW_DIR / "spark" / "databricks"

# Ensure the databricks directory is on sys.path for dbt_common imports.
if str(_DATABRICKS_DIR) not in sys.path:
    sys.path.insert(0, str(_DATABRICKS_DIR))


# ═══════════════════════════════════════════════════════════════════════════
# Helper: capture logger output
# ═══════════════════════════════════════════════════════════════════════════


@pytest.fixture
def caplog_export(caplog):
    """Configure caplog to capture logger messages from the modules under test."""
    caplog.set_level("DEBUG")
    return caplog


# ═══════════════════════════════════════════════════════════════════════════
# 1. export_csv_azure
# ═══════════════════════════════════════════════════════════════════════════


class TestExportCSVAzureConstants:
    """Verify module-level table lists — no mocking needed."""

    def _import_constants(self):
        """Lazy-import constants from export_csv_azure."""
        from plugins.operators.export_csv_azure import (
            ALL_TABLES,
            MART_TABLES,
            PUBLIC_TABLES,
            STAGING_TABLES,
            STAR_SCHEMA_DIR,
        )

        return STAGING_TABLES, MART_TABLES, PUBLIC_TABLES, ALL_TABLES, STAR_SCHEMA_DIR

    def test_staging_tables_has_10_entries(self):
        """STAGING_TABLES contains exactly 10 tables."""
        staging, _, _, _, _ = self._import_constants()
        assert len(staging) == 10
        for t in staging:
            assert t.startswith("staging.")

    def test_mart_tables_has_6_entries(self):
        """MART_TABLES contains exactly 6 tables."""
        _, marts, _, _, _ = self._import_constants()
        assert len(marts) == 6
        for t in marts:
            assert t.startswith("marts.")

    def test_public_tables_has_2_entries(self):
        """PUBLIC_TABLES contains exactly 2 dbo tables."""
        _, _, public, _, _ = self._import_constants()
        assert len(public) == 2
        for t in public:
            assert t.startswith("dbo.")

    def test_all_tables_is_sum_of_parts(self):
        """ALL_TABLES equals staging + marts + public = 18."""
        staging, marts, public, all_tables, _ = self._import_constants()
        assert all_tables == staging + marts + public
        assert len(all_tables) == 18

    def test_all_tables_contains_expected_names(self):
        """Verify expected table names are present."""
        _, _, _, all_tables, _ = self._import_constants()
        expected = {
            "staging.fact_webrequest",
            "staging.dim_date",
            "staging.dim_page",
            "staging.dim_status",
            "marts.mart_page_performance",
            "marts.mart_daily_aggregates",
            "dbo.dim_geolocation",
            "dbo.dim_useragent",
        }
        assert expected.issubset(set(all_tables)), f"Missing: {expected - set(all_tables)}"

    def test_star_schema_dir(self):
        """STAR_SCHEMA_DIR points to the expected Power BI directory."""
        _, _, _, _, schema_dir = self._import_constants()
        assert schema_dir == "/opt/airflow/data/Star-Schema"


class TestExportCSVAzure:
    """Functional tests for ``export_csv_azure()`` and ``_export_table()``."""

    # ── Helpers ──────────────────────────────────────────────────────────

    @pytest.fixture(autouse=True)
    def _ensure_module_loaded(self):
        """Ensure source module is cached in sys.modules before tests run.
        The module has no top-level Airflow or pyodbc imports, so this is
        straightforward.
        """
        from plugins.operators.export_csv_azure import (
            _export_table,
            export_csv_azure,
        )

        self._export_csv_azure = export_csv_azure
        self._export_table = _export_table
        yield

    def _clean_env(self):
        """Return a dict of env vars to restore after test."""
        saved = {}
        for k in ("AZURE_SQL_SERVER", "AZURE_SQL_DATABASE", "AZURE_SQL_USER", "AZURE_SQL_PASS"):
            saved[k] = os.environ.pop(k, None)
        return saved

    def _restore_env(self, saved):
        for k, v in saved.items():
            if v is not None:
                os.environ[k] = v
            else:
                os.environ.pop(k, None)

    def _set_creds(self):
        os.environ["AZURE_SQL_SERVER"] = "myserver.database.windows.net"
        os.environ["AZURE_SQL_DATABASE"] = "w3c-etl-db"
        os.environ["AZURE_SQL_USER"] = "admin"
        os.environ["AZURE_SQL_PASS"] = "s3cret"

    # ── Mock pyodbc + pandas context manager ────────────────────────────

    @contextmanager
    def _mock_pyodbc_pandas(self, fail_tables=None, conn_fail=False):
        """Mock ``pyodbc`` and ``pandas`` in ``sys.modules``.

        Yields ``(mock_pyodbc, mock_pandas, mock_conn)`` so callers can
        inspect call args.
        """
        mock_pyodbc = MagicMock()
        mock_conn = MagicMock()
        mock_pyodbc.connect.return_value.__enter__.return_value = mock_conn
        mock_pyodbc.connect.return_value.__exit__.return_value = None

        mock_pandas = MagicMock()
        mock_df = MagicMock()
        mock_df.__len__.return_value = 42
        mock_df.shape = (42, 12)

        mock_pandas.read_sql.return_value = mock_df

        if conn_fail:
            mock_pyodbc.Error = type("PyodbcError", (Exception,), {})
            mock_pyodbc.connect.side_effect = mock_pyodbc.Error("Connection refused")

        if fail_tables:

            def read_sql_side_effect(sql, conn):
                for ft in fail_tables:
                    if ft.replace(".", ".") in sql:
                        raise RuntimeError(f"Error reading {sql}")
                return mock_df

            mock_pandas.read_sql.side_effect = read_sql_side_effect

        with patch.dict("sys.modules", {"pyodbc": mock_pyodbc, "pandas": mock_pandas}):
            yield mock_pyodbc, mock_pandas, mock_conn, mock_df

    # ── Tests for export_csv_azure() ────────────────────────────────────

    def test_missing_creds_skips_export(self, caplog_export):
        """Missing AZURE_SQL_* env vars → warning, returns early."""
        saved = self._clean_env()
        try:
            self._export_csv_azure()
            assert any("credentials not configured" in msg for msg in caplog_export.messages)
        finally:
            self._restore_env(saved)

    def test_missing_username_also_skips(self, caplog_export):
        """Missing AZURE_SQL_USER (only) also triggers early return."""
        saved = self._clean_env()
        try:
            os.environ["AZURE_SQL_SERVER"] = "s"
            os.environ["AZURE_SQL_PASS"] = "p"
            self._export_csv_azure()
            assert any("credentials not configured" in msg for msg in caplog_export.messages)
        finally:
            self._restore_env(saved)

    def test_pyodbc_not_installed_graceful(self, caplog_export):
        """pyodbc ImportError → warning, returns."""
        saved = self._clean_env()
        old_pyodbc = sys.modules.pop("pyodbc", None)
        try:
            self._set_creds()
            original_import = builtins.__import__

            def _mock_import(name, *args, **kwargs):
                if name == "pyodbc":
                    raise ImportError("No module named pyodbc")
                return original_import(name, *args, **kwargs)

            with patch("builtins.__import__", side_effect=_mock_import):
                self._export_csv_azure()
            assert any("pyodbc not installed" in msg for msg in caplog_export.messages)
        finally:
            self._restore_env(saved)
            if old_pyodbc is not None:
                sys.modules["pyodbc"] = old_pyodbc

    def test_successful_full_export(self):
        """All 18 tables exported successfully → no error, star schema dir created."""
        saved = self._clean_env()
        try:
            self._set_creds()
            with self._mock_pyodbc_pandas() as (mock_pyodbc, mock_pandas, mock_conn, mock_df):
                with patch("plugins.operators.export_csv_azure.os.makedirs") as mock_makedirs:
                    self._export_csv_azure()

            # Star schema dir created
            mock_makedirs.assert_called_once_with("/opt/airflow/data/Star-Schema", exist_ok=True)

            # pyodbc.connect called with correct connection string
            mock_pyodbc.connect.assert_called_once()
            conn_str = mock_pyodbc.connect.call_args[0][0]
            assert "myserver.database.windows.net" in conn_str
            assert "w3c-etl-db" in conn_str
            assert "DRIVER={ODBC Driver 18 for SQL Server}" in conn_str

            # 18 tables exported
            assert mock_pandas.read_sql.call_count == 18
            assert mock_df.to_csv.call_count == 18
        finally:
            self._restore_env(saved)

    def test_public_tables_use_dbo_prefix_in_filename(self):
        """dbo.* tables are written with 'public.' filename prefix."""
        saved = self._clean_env()
        try:
            self._set_creds()
            with self._mock_pyodbc_pandas() as (_, _, _, _):
                with patch("plugins.operators.export_csv_azure.os.makedirs"):
                    with patch("plugins.operators.export_csv_azure._export_table") as mock_export:
                        self._export_csv_azure()

            # Check public table CSV paths
            public_calls = [call for call in mock_export.call_args_list if "dbo.dim" in call[0][1]]
            assert len(public_calls) == 2
            for call_args in public_calls:
                csv_path = call_args[0][2]
                assert "public." in csv_path, f"Expected 'public.' prefix in {csv_path}"
                assert "dbo." not in csv_path, f"Unexpected 'dbo.' in path {csv_path}"
        finally:
            self._restore_env(saved)

    # Note: The outer `failures` tracking in `export_csv_azure()` wraps
    # `_export_table()` calls in try/except Exception, but `_export_table()`
    # itself catches all Exception internally (logging a warning). This means
    # the outer failure tracking is currently dead code — table-level failures
    # never propagate to the outer handler. Only connection-level errors
    # (caught by `except pyodbc.Error`) can propagate.

    def test_connection_error_propagates(self):
        """pyodbc.Error at connection level → caught, logged, re-raised."""
        saved = self._clean_env()
        try:
            self._set_creds()
            with self._mock_pyodbc_pandas(conn_fail=True):
                with patch("plugins.operators.export_csv_azure.os.makedirs"):
                    with pytest.raises(Exception, match="Connection refused"):
                        self._export_csv_azure()
        finally:
            self._restore_env(saved)

    # ── Tests for _export_table() ──────────────────────────────────────

    def test_export_table_calls_pandas(self):
        """_export_table reads via pandas.read_sql and writes to_csv."""
        mock_conn = MagicMock()
        mock_pandas = MagicMock()
        mock_df = MagicMock()
        mock_df.__len__.return_value = 10
        mock_pandas.read_sql.return_value = mock_df

        with patch.dict("sys.modules", {"pandas": mock_pandas}):
            self._export_table(mock_conn, "staging.fact_webrequest", "/tmp/test.csv")

        mock_pandas.read_sql.assert_called_once_with("SELECT * FROM staging.fact_webrequest", mock_conn)
        mock_df.to_csv.assert_called_once_with("/tmp/test.csv", index=False)

    def test_export_table_logs_warning_and_raises_on_failure(self, caplog_export):
        """_export_table logs warning and re-raises so caller can track failures."""
        mock_conn = MagicMock()
        mock_pandas = MagicMock()
        mock_pandas.read_sql.side_effect = RuntimeError("DB timeout")

        with patch.dict("sys.modules", {"pandas": mock_pandas}):
            with pytest.raises(RuntimeError, match="DB timeout"):
                self._export_table(mock_conn, "staging.bad_table", "/tmp/bad.csv")

        assert any("Failed to export staging.bad_table" in msg for msg in caplog_export.messages)
        assert any("DB timeout" in msg for msg in caplog_export.messages)

    def test_export_table_with_zero_rows(self):
        """Exporting empty table still writes CSV with header."""
        mock_conn = MagicMock()
        mock_pandas = MagicMock()
        mock_df = MagicMock()
        mock_df.__len__.return_value = 0
        mock_pandas.read_sql.return_value = mock_df

        with patch.dict("sys.modules", {"pandas": mock_pandas}):
            self._export_table(mock_conn, "staging.empty_table", "/tmp/empty.csv")

        mock_df.to_csv.assert_called_once_with("/tmp/empty.csv", index=False)


# ═══════════════════════════════════════════════════════════════════════════
# 2. export_dbt_docs_azure
# ═══════════════════════════════════════════════════════════════════════════


class TestExportDBTDocsAzureConstants:
    """Module-level constants for dbt docs export."""

    def _import_constants(self):
        from plugins.operators.export_dbt_docs_azure import (
            BLOB_PREFIX,
            GOLD_CONTAINER,
            LOCAL_DOCS_DIR,
            REQUIRED_FILES,
        )

        return REQUIRED_FILES, LOCAL_DOCS_DIR, GOLD_CONTAINER, BLOB_PREFIX

    def test_required_files(self):
        """REQUIRED_FILES lists the three expected artifacts."""
        files, _, _, _ = self._import_constants()
        assert files == ["index.html", "manifest.json", "catalog.json"]

    def test_docs_dir(self):
        """LOCAL_DOCS_DIR is set to the expected path."""
        _, docs_dir, _, _ = self._import_constants()
        assert docs_dir == "/opt/airflow/data/dbt-docs"

    def test_gold_container(self):
        """GOLD_CONTAINER is 'gold'."""
        _, _, container, _ = self._import_constants()
        assert container == "gold"

    def test_blob_prefix(self):
        """BLOB_PREFIX is 'dbt-docs'."""
        _, _, _, prefix = self._import_constants()
        assert prefix == "dbt-docs"


class TestExportDBTDocsAzure:
    """Functional tests for dbt docs export operator."""

    # ── Helpers ──────────────────────────────────────────────────────────

    @contextmanager
    def _mock_azure_sdk(self, blob_service_cls=None):
        """Mock the ``azure.storage.blob`` package in ``sys.modules``.

        The ``_download_with_azure_sdk`` function imports
        ``BlobServiceClient`` inside a ``try`` block, so we inject a mock
        module hierarchy to satisfy that import without requiring the actual
        ``azure-storage-blob`` package.
        """
        mock_blob = MagicMock(name="azure.storage.blob")
        if blob_service_cls is not None:
            mock_blob.BlobServiceClient = blob_service_cls

        saved = {}
        for key in ("azure", "azure.storage", "azure.storage.blob"):
            saved[key] = sys.modules.get(key)

        sys.modules["azure"] = MagicMock(name="azure")
        sys.modules["azure.storage"] = MagicMock(name="azure.storage")
        # Wire submodule access so ``from azure.storage.blob import X`` works
        sys.modules["azure.storage"].blob = mock_blob
        sys.modules["azure.storage.blob"] = mock_blob

        try:
            yield mock_blob
        finally:
            for key in ("azure", "azure.storage", "azure.storage.blob"):
                if saved[key] is not None:
                    sys.modules[key] = saved[key]
                else:
                    sys.modules.pop(key, None)

    @pytest.fixture(autouse=True)
    def _ensure_module_loaded(self):
        from plugins.operators.export_dbt_docs_azure import (
            _download_with_azure_sdk,
            _verify_docs,
            export_dbt_docs_to_airflow,
        )

        self._export_dbt_docs_to_airflow = export_dbt_docs_to_airflow
        self._download_with_azure_sdk = _download_with_azure_sdk
        self._verify_docs = _verify_docs
        yield

    def _assert_log_contains(self, caplog, fragment):
        assert any(fragment in msg for msg in caplog.messages), (
            f"Expected log containing {fragment!r}, got: {caplog.messages}"
        )

    # ── _verify_docs() ──────────────────────────────────────────────────

    def test_verify_docs_all_present(self):
        """All 3 files present + valid catalog.json → True."""
        with patch("os.path.isfile", return_value=True):
            with patch("builtins.open", mock_open(read_data='{"nodes": {}}')):
                assert self._verify_docs() is True

    def test_verify_docs_missing_file(self):
        """Missing file → False."""

        def fake_isfile(path):
            return "catalog.json" not in path

        with patch("os.path.isfile", side_effect=fake_isfile):
            with patch("builtins.open", mock_open(read_data="{}")):
                assert self._verify_docs() is False

    def test_verify_docs_invalid_json(self, caplog_export):
        """Invalid catalog.json → False, warning logged."""
        with patch("os.path.isfile", return_value=True):
            with patch("builtins.open", mock_open(read_data="not json")):
                assert self._verify_docs() is False
            self._assert_log_contains(caplog_export, "catalog.json is invalid")

    def test_verify_docs_empty_directory(self):
        """No files present → False."""
        with patch("os.path.isfile", return_value=False):
            assert self._verify_docs() is False

    # ── _download_with_azure_sdk() ──────────────────────────────────────

    def test_sdk_not_installed(self, caplog_export):
        """azure-storage-blob ImportError → warning, returns None."""
        # Simulate missing azure by patching sys.modules to remove it
        saved_azure = sys.modules.pop("azure", None)
        saved_storage = sys.modules.pop("azure.storage", None)
        saved_blob = sys.modules.pop("azure.storage.blob", None)
        try:
            self._download_with_azure_sdk("acct", "key")
        finally:
            if saved_azure is not None:
                sys.modules["azure"] = saved_azure
            if saved_storage is not None:
                sys.modules["azure.storage"] = saved_storage
            if saved_blob is not None:
                sys.modules["azure.storage.blob"] = saved_blob
        self._assert_log_contains(caplog_export, "azure-storage-blob SDK not installed")

    def test_sdk_download_success(self):
        """Full SDK flow with all files downloaded."""
        mock_blob_client = MagicMock()
        mock_blob_client.download_blob.return_value.readall.return_value = b"content"

        mock_container_client = MagicMock()
        mock_container_client.get_container_properties.return_value = None
        mock_container_client.get_blob_client.return_value = mock_blob_client

        mock_blob_service = MagicMock()
        mock_blob_service.from_connection_string.return_value.get_container_client.return_value = mock_container_client

        with self._mock_azure_sdk(blob_service_cls=mock_blob_service):
            with patch("builtins.open", mock_open()):
                self._download_with_azure_sdk("acct", "key")

        assert mock_blob_client.download_blob.call_count == 3

    def test_sdk_container_not_found(self, caplog_export):
        """Container not accessible → warning, no download."""
        mock_container_client = MagicMock()
        mock_container_client.get_container_properties.side_effect = Exception("Not found")

        mock_blob_service = MagicMock()
        mock_blob_service.from_connection_string.return_value.get_container_client.return_value = mock_container_client

        with self._mock_azure_sdk(blob_service_cls=mock_blob_service):
            self._download_with_azure_sdk("acct", "key")

        self._assert_log_contains(caplog_export, "not found or not accessible")

    def test_sdk_partial_download(self, caplog_export):
        """Some blobs fail → warning, continues to next."""
        mock_container_client = MagicMock()
        mock_container_client.get_container_properties.return_value = None

        def blob_side_effect(path):
            if "manifest" in path:
                raise Exception("Blob not found")
            mock = MagicMock()
            mock.download_blob().readall.return_value = b"ok"
            return mock

        mock_container_client.get_blob_client.side_effect = blob_side_effect

        mock_blob_service = MagicMock()
        mock_blob_service.from_connection_string.return_value.get_container_client.return_value = mock_container_client

        with self._mock_azure_sdk(blob_service_cls=mock_blob_service):
            with patch("builtins.open", mock_open()):
                self._download_with_azure_sdk("acct", "key")

        self._assert_log_contains(caplog_export, "Could not download")

    # ── export_dbt_docs_to_airflow() ────────────────────────────────────

    def test_no_storage_creds_falls_through(self, caplog_export):
        """No STORAGE_ACCOUNT_NAME/STORAGE_ACCESS_KEY → info log, local check."""
        saved = os.environ.pop("STORAGE_ACCOUNT_NAME", None), os.environ.pop("STORAGE_ACCESS_KEY", None)
        try:
            # Both no creds and no local docs → warning
            with patch("plugins.operators.export_dbt_docs_azure._verify_docs", return_value=False):
                with patch("plugins.operators.export_dbt_docs_azure.os.makedirs"):
                    self._export_dbt_docs_to_airflow()

            self._assert_log_contains(caplog_export, "not configured")
            self._assert_log_contains(caplog_export, "not downloaded")
        finally:
            if saved[0] is not None:
                os.environ["STORAGE_ACCOUNT_NAME"] = saved[0]
            if saved[1] is not None:
                os.environ["STORAGE_ACCESS_KEY"] = saved[1]

    def test_sdk_success_returns_early(self):
        """SDK download succeeds → returns, no fallback needed."""
        saved = os.environ.pop("STORAGE_ACCOUNT_NAME", None), os.environ.pop("STORAGE_ACCESS_KEY", None)
        try:
            os.environ["STORAGE_ACCOUNT_NAME"] = "myacct"
            os.environ["STORAGE_ACCESS_KEY"] = "mykey"

            with patch("plugins.operators.export_dbt_docs_azure._download_with_azure_sdk") as mock_dl:
                with patch("plugins.operators.export_dbt_docs_azure._verify_docs", return_value=True):
                    with patch("plugins.operators.export_dbt_docs_azure.os.makedirs"):
                        self._export_dbt_docs_to_airflow()

            mock_dl.assert_called_once_with("myacct", "mykey")
        finally:
            if saved[1] is not None:
                os.environ["STORAGE_ACCESS_KEY"] = saved[1]
            if saved[0] is not None:
                os.environ["STORAGE_ACCOUNT_NAME"] = saved[0]

    def test_sdk_failure_falls_back_to_local(self, caplog_export):
        """SDK fails → tries local, succeeds there."""
        saved = os.environ.pop("STORAGE_ACCOUNT_NAME", None), os.environ.pop("STORAGE_ACCESS_KEY", None)
        try:
            os.environ["STORAGE_ACCOUNT_NAME"] = "myacct"
            os.environ["STORAGE_ACCESS_KEY"] = "mykey"

            with patch("plugins.operators.export_dbt_docs_azure._download_with_azure_sdk") as mock_dl:
                mock_dl.side_effect = Exception("SDK download failed")
                with patch(
                    "plugins.operators.export_dbt_docs_azure._verify_docs",
                    return_value=True,
                ):
                    with patch("plugins.operators.export_dbt_docs_azure.os.makedirs"):
                        self._export_dbt_docs_to_airflow()

            self._assert_log_contains(caplog_export, "SDK download failed")
            self._assert_log_contains(caplog_export, "already present")
        finally:
            if saved[1] is not None:
                os.environ["STORAGE_ACCESS_KEY"] = saved[1]
            if saved[0] is not None:
                os.environ["STORAGE_ACCOUNT_NAME"] = saved[0]

    def test_all_methods_fail_graceful(self, caplog_export):
        """Both methods fail → warning about remaining on DBFS."""
        saved = os.environ.pop("STORAGE_ACCOUNT_NAME", None), os.environ.pop("STORAGE_ACCESS_KEY", None)
        try:
            with patch("plugins.operators.export_dbt_docs_azure._verify_docs", return_value=False):
                with patch("plugins.operators.export_dbt_docs_azure.os.makedirs"):
                    self._export_dbt_docs_to_airflow()

            self._assert_log_contains(caplog_export, "remain available on Databricks DBFS")
        finally:
            if saved[0] is not None:
                os.environ["STORAGE_ACCOUNT_NAME"] = saved[0]
            if saved[1] is not None:
                os.environ["STORAGE_ACCESS_KEY"] = saved[1]

    # ── Databricks-convention env var tests ──────────────────────────────

    def test_databricks_env_vars_used_when_set(self):
        """AZURE_STORAGE_ACCOUNT / AZURE_STORAGE_KEY → SDK called with those values."""
        for k in ("STORAGE_ACCOUNT_NAME", "STORAGE_ACCESS_KEY", "AZURE_STORAGE_ACCOUNT", "AZURE_STORAGE_KEY"):
            os.environ.pop(k, None)
        try:
            os.environ["AZURE_STORAGE_ACCOUNT"] = "databricks-acct"
            os.environ["AZURE_STORAGE_KEY"] = "databricks-key"

            with patch("plugins.operators.export_dbt_docs_azure._download_with_azure_sdk") as mock_dl:
                with patch("plugins.operators.export_dbt_docs_azure._verify_docs", return_value=True):
                    with patch("plugins.operators.export_dbt_docs_azure.os.makedirs"):
                        self._export_dbt_docs_to_airflow()

            mock_dl.assert_called_once_with("databricks-acct", "databricks-key")
        finally:
            for k in ("AZURE_STORAGE_ACCOUNT", "AZURE_STORAGE_KEY", "STORAGE_ACCOUNT_NAME", "STORAGE_ACCESS_KEY"):
                os.environ.pop(k, None)

    def test_databricks_names_take_priority(self):
        """Both conventions set → AZURE_STORAGE_* takes priority."""
        for k in ("STORAGE_ACCOUNT_NAME", "STORAGE_ACCESS_KEY", "AZURE_STORAGE_ACCOUNT", "AZURE_STORAGE_KEY"):
            os.environ.pop(k, None)
        try:
            os.environ["AZURE_STORAGE_ACCOUNT"] = "priority-acct"
            os.environ["AZURE_STORAGE_KEY"] = "priority-key"
            os.environ["STORAGE_ACCOUNT_NAME"] = "legacy-acct"
            os.environ["STORAGE_ACCESS_KEY"] = "legacy-key"

            with patch("plugins.operators.export_dbt_docs_azure._download_with_azure_sdk") as mock_dl:
                with patch("plugins.operators.export_dbt_docs_azure._verify_docs", return_value=True):
                    with patch("plugins.operators.export_dbt_docs_azure.os.makedirs"):
                        self._export_dbt_docs_to_airflow()

            mock_dl.assert_called_once_with("priority-acct", "priority-key")
        finally:
            for k in ("AZURE_STORAGE_ACCOUNT", "AZURE_STORAGE_KEY", "STORAGE_ACCOUNT_NAME", "STORAGE_ACCESS_KEY"):
                os.environ.pop(k, None)


# ═══════════════════════════════════════════════════════════════════════════
# 3. dbt_common
# ═══════════════════════════════════════════════════════════════════════════


class TestDBTCommonWriteProfiles:
    """Tests for ``dbt_common._write_profiles_yml()`` — pure YAML generation."""

    def _import_func(self):
        from dbt_common import _write_profiles_yml

        return _write_profiles_yml

    def test_writes_profiles_yml(self, tmp_path):
        """_write_profiles_yml produces a valid profiles.yml with env var content."""
        os.environ["AZURE_SQL_SERVER"] = "test-server.database.windows.net"
        os.environ["AZURE_SQL_DB"] = "test-db"
        os.environ["AZURE_SQL_USER"] = "test-user"
        os.environ["AZURE_SQL_PASSWORD"] = "test-pass"

        try:
            write_profiles = self._import_func()
            result_path = write_profiles(str(tmp_path))

            assert result_path == str(tmp_path / "profiles.yml")
            content = (tmp_path / "profiles.yml").read_text()

            assert "w3c_azure:" in content
            assert "type: sqlserver" in content
            assert "driver: ODBC Driver 18 for SQL Server" in content
            assert "test-server.database.windows.net" in content
            assert "test-db" in content
            assert "test-user" in content
            assert "test-pass" in content
            assert "threads: 4" in content
            assert "retries: 3" in content
            assert "encrypt: true" in content
            assert "trust_cert: false" in content
        finally:
            os.environ.pop("AZURE_SQL_SERVER", None)
            os.environ.pop("AZURE_SQL_DB", None)
            os.environ.pop("AZURE_SQL_USER", None)
            os.environ.pop("AZURE_SQL_PASSWORD", None)

    def test_profiles_yml_default_database(self, tmp_path):
        """Default database name is 'w3c_etl' when AZURE_SQL_DB is unset."""
        os.environ["AZURE_SQL_SERVER"] = "srv"
        os.environ["AZURE_SQL_USER"] = "u"
        os.environ["AZURE_SQL_PASSWORD"] = "p"
        os.environ.pop("AZURE_SQL_DB", None)

        try:
            write_profiles = self._import_func()
            write_profiles(str(tmp_path))
            content = (tmp_path / "profiles.yml").read_text()
            assert "database: w3c_etl" in content
        finally:
            os.environ.pop("AZURE_SQL_SERVER", None)
            os.environ.pop("AZURE_SQL_USER", None)
            os.environ.pop("AZURE_SQL_PASSWORD", None)

    def test_profiles_yml_empty_strings_when_missing(self, tmp_path):
        """Missing env vars result in empty strings (not crashes)."""
        os.environ.pop("AZURE_SQL_SERVER", None)
        os.environ.pop("AZURE_SQL_DB", None)
        os.environ.pop("AZURE_SQL_USER", None)
        os.environ.pop("AZURE_SQL_PASSWORD", None)

        write_profiles = self._import_func()
        write_profiles(str(tmp_path))

        content = (tmp_path / "profiles.yml").read_text()
        assert 'server: ""' in content
        # AZURE_SQL_USER and AZURE_SQL_PASSWORD default to "" which
        # formats as empty values in the YAML (``user: \n``, not ``user: ""``)
        assert "user: " in content and 'user: ""' not in content
        assert "password: " in content and 'password: ""' not in content


class TestDBTCommonRunCommand:
    """Tests for ``dbt_common.run_dbt_command()``."""

    def _import_func(self):
        from dbt_common import run_dbt_command

        return run_dbt_command

    def test_appends_profile_flags(self, tmp_path):
        """run_dbt_command appends --profile w3c_azure --profiles-dir."""
        project_dir = tmp_path / "project"
        project_dir.mkdir()
        (project_dir / "dbt_project.yml").write_text("")
        run_dbt = self._import_func()
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = "done"
        mock_result.stderr = ""

        with patch("dbt_common.subprocess.run", return_value=mock_result) as mock_run:
            run_dbt(["dbt", "run"], str(project_dir), "run")

        mock_run.assert_called_once()
        args = mock_run.call_args[0][0]
        assert "--profile" in args
        assert "w3c_azure" in args
        assert "--profiles-dir" in args
        assert str(project_dir) in args

    def test_returns_completed_process(self, tmp_path):
        """Successful run returns the CompletedProcess."""
        project_dir = tmp_path / "project"
        project_dir.mkdir()
        (project_dir / "dbt_project.yml").write_text("")
        run_dbt = self._import_func()
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = "all good"
        mock_result.stderr = ""

        with patch("dbt_common.subprocess.run", return_value=mock_result):
            result = run_dbt(["dbt", "run"], str(project_dir), "run")

        assert result is mock_result

    def test_non_zero_exit_raises(self, tmp_path):
        """Non-zero returncode → RuntimeError with stdout/stderr."""
        project_dir = tmp_path / "project"
        project_dir.mkdir()
        (project_dir / "dbt_project.yml").write_text("")
        run_dbt = self._import_func()
        mock_result = MagicMock()
        mock_result.returncode = 1
        mock_result.stdout = "error details"
        mock_result.stderr = "some stderr"

        with patch("dbt_common.subprocess.run", return_value=mock_result):
            with pytest.raises(RuntimeError) as excinfo:
                run_dbt(["dbt", "run"], str(project_dir), "run")

        assert "dbt run failed" in str(excinfo.value)
        assert "error details" in str(excinfo.value)

    def test_custom_command_name_in_error(self, tmp_path):
        """Custom command_name appears in error message."""
        project_dir = tmp_path / "project"
        project_dir.mkdir()
        (project_dir / "dbt_project.yml").write_text("")
        run_dbt = self._import_func()
        mock_result = MagicMock()
        mock_result.returncode = 2
        mock_result.stdout = ""
        mock_result.stderr = ""

        with patch("dbt_common.subprocess.run", return_value=mock_result):
            with pytest.raises(RuntimeError) as excinfo:
                run_dbt(["dbt", "docs", "generate"], str(project_dir), "docs generate")

        assert "dbt docs generate failed" in str(excinfo.value)


class TestDBTCommonCleanup:
    """Tests for ``dbt_common.cleanup()``."""

    def _import_func(self):
        from dbt_common import cleanup

        return cleanup

    def test_cleanup_removes_directory(self):
        """cleanup calls shutil.rmtree on the given path."""
        cleanup_fn = self._import_func()

        with patch("dbt_common.shutil.rmtree") as mock_rmtree:
            cleanup_fn("/tmp/dbt_xyz123")

        mock_rmtree.assert_called_once_with("/tmp/dbt_xyz123", ignore_errors=True)

    def test_cleanup_does_not_raise_on_missing_dir(self):
        """cleanup is safe to call multiple times — no error."""
        cleanup_fn = self._import_func()

        with patch("dbt_common.shutil.rmtree") as mock_rmtree:
            # First call
            cleanup_fn("/tmp/missing_dir")
            # Second call — no error
            cleanup_fn("/tmp/missing_dir")

        assert mock_rmtree.call_count == 2
