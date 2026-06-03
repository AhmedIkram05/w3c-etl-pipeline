"""
Integration tests for the pipeline: Spark → PostgreSQL → dbt → mart validation.

These tests require a running Docker stack with Airflow, Spark, and PostgreSQL.
They are skipped by default with 'Make Test'

    # Direct invocation:
    docker compose exec airflow-scheduler \\
        python -m pytest tests/test_integration.py -v -m integration

    # From host with full Docker stack running:
    docker compose -f airflow/docker-compose.yaml exec -T airflow-scheduler \\
        python -m pytest tests/test_integration.py -v -m integration
"""

import os
import subprocess

import pytest

# dbt paths (inside the container)
_DBT_PROJECT = "/opt/airflow/dbt/w3c"
_DBT_PROFILES = "/opt/airflow/dbt"
# Compose file path (used when running tests from outside Docker)
_COMPOSE_FILE = "/opt/airflow/docker-compose.yaml"

# Check if we're inside Docker (container ID file exists)
_INSIDE_DOCKER = os.path.exists("/.dockerenv") or os.path.exists("/proc/1/cgroup")
# Integration tests require a running Docker stack
INTEGRATION_TESTS_DISABLED = not _INSIDE_DOCKER


def _psql(database: str, query: str, timeout: int = 30) -> str:
    """Execute a PostgreSQL query via psql and return the trimmed stdout.

    Inside Docker, uses ``psql -h postgres`` (TCP connection to the
    postgres container).  Outside Docker, uses ``docker compose exec postgres``.
    """
    env = os.environ.copy()
    env["PGPASSWORD"] = "airflow"
    if _INSIDE_DOCKER:
        cmd = [
            "psql",
            "-h",
            "postgres",
            "-U",
            "airflow",
            "-d",
            database,
            "-t",
            "-c",
            query,
        ]
    else:
        cmd = [
            "docker",
            "compose",
            "-f",
            _COMPOSE_FILE,
            "exec",
            "-T",
            "postgres",
            "psql",
            "-U",
            "airflow",
            "-d",
            database,
            "-t",
            "-c",
            query,
        ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, env=env)
    if result.returncode != 0:
        pytest.fail(f"psql failed (exit {result.returncode}): {result.stderr}")
    return result.stdout.strip()


def _run(cmd: list[str], timeout: int = 120) -> subprocess.CompletedProcess:
    """Run a command directly (inside Docker, no docker compose wrapper)."""
    return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)


# ═══════════════════════════════════════════════════════════════════════════
#  Mark all tests in this module as integration
# ═══════════════════════════════════════════════════════════════════════════

pytestmark = pytest.mark.integration


# ═══════════════════════════════════════════════════════════════════════════
#  Export Warehouse Tests
# ═══════════════════════════════════════════════════════════════════════════


class TestExportWarehouse:
    """Verify export_warehouse.py reads Delta silver and writes to PostgreSQL."""

    def test_raw_enriched_table_exists(self):
        """Check raw_enriched table exists in PostgreSQL."""
        result = _psql(
            "w3c_warehouse",
            "SELECT EXISTS (SELECT FROM information_schema.tables "
            "WHERE table_schema = 'public' AND table_name = 'raw_enriched')",
        )
        assert result.strip() == "t", "public.raw_enriched table does not exist"

    def test_raw_enriched_populated(self):
        """Check raw_enriched table has at least one row after export."""
        count = int(_psql("w3c_warehouse", "SELECT COUNT(*) FROM public.raw_enriched"))
        assert count > 0, f"raw_enriched table is empty ({count} rows). Run the spark_ingestion DAG first."

    def test_tracking_table_exists(self):
        """Check the idempotency tracking table exists."""
        result = _psql(
            "w3c_warehouse",
            "SELECT EXISTS (SELECT FROM information_schema.tables "
            "WHERE table_schema = 'public' AND table_name = 'raw_enriched_loaded')",
        )
        assert result.strip() == "t", "public.raw_enriched_loaded table does not exist"

    def test_raw_enriched_has_type_cast_columns(self):
        """Verify the type-cast columns exist with correct PostgreSQL types."""
        columns = [
            ("is_crawler", "boolean"),
            ("bytes_sent", "bigint"),
            ("bytes_recv", "bigint"),
            ("time_taken", "bigint"),
        ]
        for col_name, expected_type in columns:
            result = _psql(
                "w3c_warehouse",
                f"SELECT data_type FROM information_schema.columns "
                f"WHERE table_schema = 'public' AND table_name = 'raw_enriched' "
                f"AND column_name = '{col_name}'",
            )
            actual_type = result.strip()
            assert actual_type == expected_type, f"Column {col_name}: expected {expected_type}, got {actual_type}"

    @pytest.mark.skipif(INTEGRATION_TESTS_DISABLED, reason="Integration tests require a running Docker stack")
    def test_fact_webrequest_has_no_dead_cols(self):
        """Verify fact_webrequest no longer has the 11 denormalized columns that were dropped."""
        sql = """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'dbt_staging'
              AND table_name = 'fact_webrequest'
        """
        result = _psql("w3c_warehouse", sql)
        columns = [c.strip() for c in result.strip().split("\n") if c.strip()]
        dead_cols = {
            "country",
            "region",
            "city",
            "latitude",
            "longitude",
            "isp",
            "agent_type",
            "browser_name",
            "browser_version",
            "operating_system",
            "device_type",
        }
        assert len(columns) == 24, f"Expected 24 columns in dbt_staging.fact_webrequest, got {len(columns)}: {columns}"
        assert dead_cols.isdisjoint(columns), f"Dead columns still present: {dead_cols & set(columns)}"


# ═══════════════════════════════════════════════════════════════════════════
#  dbt Models Tests
# ═══════════════════════════════════════════════════════════════════════════


class TestDBTModels:
    """Verify dbt models build and tests pass."""

    def test_dbt_run_succeeds(self):
        """Run dbt run and verify it completes with exit code 0."""
        result = _run(
            [
                "dbt",
                "run",
                "--project-dir",
                _DBT_PROJECT,
                "--profiles-dir",
                _DBT_PROFILES,
            ],
            timeout=180,
        )
        assert result.returncode == 0, (
            f"dbt run failed (exit {result.returncode}):\n"
            f"STDOUT: {result.stdout[:2000]}\n"
            f"STDERR: {result.stderr[:2000]}"
        )

    def test_dbt_test_passes(self):
        """Run dbt test and verify all tests pass."""
        result = _run(
            [
                "dbt",
                "test",
                "--project-dir",
                _DBT_PROJECT,
                "--profiles-dir",
                _DBT_PROFILES,
            ],
            timeout=180,
        )
        assert result.returncode == 0, (
            f"dbt test failed (exit {result.returncode}):\n"
            f"STDOUT: {result.stdout[:2000]}\n"
            f"STDERR: {result.stderr[:2000]}"
        )

    def test_dbt_docs_generates(self):
        """Run dbt docs generate and verify it succeeds."""
        result = _run(
            [
                "dbt",
                "docs",
                "generate",
                "--project-dir",
                _DBT_PROJECT,
                "--profiles-dir",
                _DBT_PROFILES,
            ],
            timeout=120,
        )
        assert result.returncode == 0, f"dbt docs generate failed (exit {result.returncode}):\n{result.stderr[:1000]}"


# ═══════════════════════════════════════════════════════════════════════════
#  Mart Verification Tests
# ═══════════════════════════════════════════════════════════════════════════


class TestMartVerification:
    """Spot-check mart tables have expected row counts and integrity."""

    MART_TABLES = [
        "dbt_marts.mart_daily_aggregates",
        "dbt_marts.mart_page_performance",
        "dbt_marts.mart_browser_analysis",
        "dbt_marts.mart_crawler_analysis",
        "dbt_marts.mart_timeofday_analysis",
    ]

    STAGING_TABLES = [
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
    ]

    def test_marts_have_rows(self):
        """Verify all 5 mart tables have at least 1 row."""
        empty_tables = []
        for table in self.MART_TABLES:
            count = int(_psql("w3c_warehouse", f"SELECT COUNT(*) FROM {table}"))
            if count == 0:
                empty_tables.append(table)
        assert not empty_tables, f"Empty mart table(s): {empty_tables}"

    def test_staging_tables_have_rows(self):
        """Verify all 10 dbt staging tables have at least 1 row."""
        empty_tables = []
        for table in self.STAGING_TABLES:
            count = int(_psql("w3c_warehouse", f"SELECT COUNT(*) FROM {table}"))
            if count == 0:
                empty_tables.append(table)
        assert not empty_tables, f"Empty staging table(s): {empty_tables}"

    def test_dimension_tables_have_default_rows(self):
        """Verify Airflow-managed dimension tables have -1 default rows."""
        for table in ["public.dim_geolocation", "public.dim_useragent"]:
            count = int(_psql("w3c_warehouse", f"SELECT COUNT(*) FROM {table}"))
            assert count > 0, f"{table} is empty — should have at least the -1 default row"

    def test_dim_geolocation_has_negative_one_default(self):
        """Verify dim_geolocation has the -1 surrogate key row."""
        count = int(_psql("w3c_warehouse", "SELECT COUNT(*) FROM public.dim_geolocation WHERE geolocation_sk = -1"))
        assert count == 1, "dim_geolocation missing -1 default row"

    def test_dim_useragent_has_negative_one_default(self):
        """Verify dim_useragent has the -1 surrogate key row."""
        count = int(_psql("w3c_warehouse", "SELECT COUNT(*) FROM public.dim_useragent WHERE user_agent_sk = -1"))
        assert count == 1, "dim_useragent missing -1 default row"

    def test_fact_webrequest_references_dimensions(self):
        """Verify fact_webrequest FK references resolve correctly."""
        # Check at least one fact row has a valid dimension reference
        result = _psql(
            "w3c_warehouse",
            """
            SELECT COUNT(*) FROM dbt_staging.fact_webrequest f
            LEFT JOIN dbt_staging.dim_date     d  ON f.date_sk = d.date_sk
            LEFT JOIN dbt_staging.dim_time     t  ON f.time_sk = t.time_sk
            LEFT JOIN dbt_staging.dim_page     p  ON f.page_sk = p.page_sk
            LEFT JOIN dbt_staging.dim_method   m  ON f.method_sk = m.method_sk
            LEFT JOIN dbt_staging.dim_status   s  ON f.status_sk = s.status_sk
            LEFT JOIN dbt_staging.dim_referrer r  ON f.referrer_sk = r.referrer_sk
            LEFT JOIN dbt_staging.dim_visitortype v ON f.visitor_sk = v.visitor_sk
            WHERE f.date_sk IS NOT NULL
        """,
        )
        count = int(result)
        assert count > 0, "No fact_webrequest rows have valid dimension references. Check dbt model integrity."


# ═══════════════════════════════════════════════════════════════════════════
#  Airflow DAG Tests
# ═══════════════════════════════════════════════════════════════════════════


class TestAirflowDAGs:
    """Verify Airflow DAGs have run successfully."""

    def test_spark_ingestion_dag_has_run(self):
        """Verify the spark_ingestion DAG has at least one successful run."""
        result = _psql(
            "airflow",
            """
            SELECT COUNT(*) FROM dag_run
            WHERE dag_id = 'w3c_spark_ingestion'
            AND state = 'success'
        """,
        )
        count = int(result)
        assert count > 0, "w3c_spark_ingestion DAG has no successful runs. Trigger it first via Airflow UI or CLI."

    def test_dbt_marts_dag_has_run(self):
        """Verify the dbt_marts DAG has at least one successful run."""
        result = _psql(
            "airflow",
            """
            SELECT COUNT(*) FROM dag_run
            WHERE dag_id = 'w3c_dbt_marts'
            AND state = 'success'
        """,
        )
        count = int(result)
        assert count > 0, (
            "dbt_marts DAG has no successful runs. It should be Dataset-triggered after spark_ingestion completes."
        )


# ═══════════════════════════════════════════════════════════════════════════
#  Delta Lake Health Tests
# ═══════════════════════════════════════════════════════════════════════════


class TestDeltaLakeHealth:
    """Verify Delta Lake tables exist and have data."""

    def test_delta_bronze_exists(self):
        """Verify the Bronze Delta directory exists (volume mounted into container)."""
        assert os.path.isdir("/opt/spark/delta/bronze"), (
            "Bronze Delta table not found at /opt/spark/delta/bronze. Run bronze_ingestion first."
        )

    def test_delta_silver_exists(self):
        """Verify the Silver Delta directory exists (volume mounted into container)."""
        assert os.path.isdir("/opt/spark/delta/silver"), (
            "Silver Delta table not found at /opt/spark/delta/silver. Run silver_enrichment first."
        )
