"""
Unit tests for Databricks UC Silver enrichment (``02_silver_enrichment.py``).

Tests cover:

* ``_geoip_lookup`` — GeoIP attribute lookup (returns "Unknown" with no DB)
* ``get_source_files_in_uc`` — loaded-file discovery from UC table
* ``ensure_silver_table`` — DDL for silver UC table
* ``discover_crawler_ips`` — crawler detection from ``robots.txt`` requests
* ``run`` — end-to-end enrichment pipeline with mock bronze table

Enrichment UDFs (page_category, referrer_domain, traffic_type, size_band)
are defined as closures inside ``run()`` and are tested indirectly by
verifying the enriched DataFrame output.

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

silver = importlib.import_module("02_silver_enrichment")


# ── Local spark fixture ────────────────────────────────────────────────────
@pytest.fixture
def spark():
    """Create a minimal local SparkSession for testing pipeline logic.

    Uses a unique warehouse directory so tables from one test do not
    collide with another test on ``saveAsTable``.

    Also adds the module under test to ``addPyFile`` so that Spark workers
    can ``importlib.import_module("02_silver_enrichment")`` when
    cloudpickle deserialises UDFs.  The module name starts with a digit,
    which is acceptable to ``importlib`` but requires the file to be on
    the worker's Python path.
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

    # Add module file so UDF cloudpickle can find it on workers
    script_dir = os.path.dirname(os.path.abspath(__file__))
    db_dir = os.path.join(script_dir, "..", "airflow", "spark", "databricks")
    _module_path = os.path.join(db_dir, "02_silver_enrichment.py")
    if os.path.exists(_module_path):
        session.sparkContext.addPyFile(_module_path)

    yield session
    session.stop()


# ══════════════════════════════════════════════════════════════════════
#  1. _geoip_lookup — returns "Unknown" when no DB is loaded
# ══════════════════════════════════════════════════════════════════════


class TestGeoIPLookup:
    """GeoIP attribute lookup with no reader available (returns "Unknown")."""

    def setup_method(self):
        """Ensure _geoip_reader is None before each test."""
        silver._geoip_reader = None

    def test_country_unknown(self):
        assert silver._geoip_lookup("8.8.8.8", "country") == "Unknown"

    def test_region_unknown(self):
        assert silver._geoip_lookup("8.8.8.8", "region") == "Unknown"

    def test_city_unknown(self):
        assert silver._geoip_lookup("8.8.8.8", "city") == "Unknown"

    def test_latitude_unknown(self):
        assert silver._geoip_lookup("8.8.8.8", "latitude") == "Unknown"

    def test_longitude_unknown(self):
        assert silver._geoip_lookup("8.8.8.8", "longitude") == "Unknown"

    def test_isp_unknown(self):
        assert silver._geoip_lookup("8.8.8.8", "isp") == "Unknown"

    def test_unknown_attr_defaults_to_unknown(self):
        assert silver._geoip_lookup("8.8.8.8", "nonexistent") == "Unknown"

    def test_empty_ip_returns_unknown(self):
        assert silver._geoip_lookup("", "country") == "Unknown"


# ══════════════════════════════════════════════════════════════════════
#  2. get_source_files_in_uc — UC table source-file discovery
# ══════════════════════════════════════════════════════════════════════


class TestGetSourceFilesInUC:
    """Verify source-file discovery from a Unity Catalog table."""

    def test_returns_empty_set_when_table_does_not_exist(self, spark):
        result = silver.get_source_files_in_uc(spark, "nonexistent.table")
        assert result == set()

    def test_returns_set_of_source_files(self, spark):
        table_name = "test_uc_source_files"
        spark.createDataFrame(
            [("f1.log",), ("f2.log",), ("f1.log",)],
            schema="source_file string",
        ).write.mode("overwrite").saveAsTable(table_name)

        result = silver.get_source_files_in_uc(spark, table_name)
        assert result == {"f1.log", "f2.log"}

        spark.sql(f"DROP TABLE IF EXISTS {table_name}")


# ══════════════════════════════════════════════════════════════════════
#  3. ensure_silver_table — DDL for silver UC table
# ══════════════════════════════════════════════════════════════════════


class TestEnsureSilverTable:
    """Verify the DDL statement creates the silver table."""

    UC_TABLE = "test_silver_ensure"

    @pytest.fixture(autouse=True)
    def _patch_uc_path(self):
        original = silver.SILVER_UC_PATH
        silver.SILVER_UC_PATH = self.UC_TABLE
        yield
        silver.SILVER_UC_PATH = original

    def test_creates_table(self, spark):
        silver.ensure_silver_table(spark)
        df = spark.table(self.UC_TABLE)
        assert df.count() == 0

    def test_has_30_columns(self, spark):
        silver.ensure_silver_table(spark)
        df = spark.table(self.UC_TABLE)
        column_names = {f.name for f in df.schema.fields}
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
            "country",
            "region",
            "city",
            "latitude",
            "longitude",
            "isp",
            "page_category",
            "referrer_domain",
            "traffic_type",
            "is_crawler",
            "size_band",
        }
        assert column_names == expected, f"Missing: {expected - column_names}"

    def test_idempotent(self, spark):
        silver.ensure_silver_table(spark)
        silver.ensure_silver_table(spark)  # second call — no error
        assert spark.table(self.UC_TABLE).schema is not None


# ══════════════════════════════════════════════════════════════════════
#  4. discover_crawler_ips — crawler detection from robots.txt
# ══════════════════════════════════════════════════════════════════════


class TestDiscoverCrawlerIPs:
    """Verify crawler IP discovery from robots.txt requests."""

    BRONZE_TABLE = "test_bronze_crawlers"

    @pytest.fixture(autouse=True)
    def _patch_bronze_path(self):
        original = silver.BRONZE_UC_PATH
        silver.BRONZE_UC_PATH = self.BRONZE_TABLE
        yield
        silver.BRONZE_UC_PATH = original

    def test_returns_empty_when_table_missing(self, spark):
        result = silver.discover_crawler_ips(spark)
        assert result == set()

    def test_detects_crawler_ip(self, spark):
        spark.createDataFrame(
            [
                ("1.2.3.4", "/robots.txt"),
                ("5.6.7.8", "/index.html"),
                ("1.2.3.4", "/robots.txt.disallow"),
                ("9.9.9.9", "/robots.txt"),
            ],
            schema="client_ip string, uri_stem string",
        ).write.mode("overwrite").saveAsTable(self.BRONZE_TABLE)

        result = silver.discover_crawler_ips(spark)
        assert "1.2.3.4" in result
        assert "5.6.7.8" not in result
        assert "9.9.9.9" in result

    def test_case_insensitive_matching(self, spark):
        spark.createDataFrame(
            [("1.2.3.4", "/Robots.Txt")],
            schema="client_ip string, uri_stem string",
        ).write.mode("overwrite").saveAsTable(self.BRONZE_TABLE)

        result = silver.discover_crawler_ips(spark)
        assert "1.2.3.4" in result


# ══════════════════════════════════════════════════════════════════════
#  5. run — end-to-end enrichment pipeline
# ══════════════════════════════════════════════════════════════════════


class TestRunEnrichment:
    """Verify the full silver enrichment pipeline."""

    BRONZE_TABLE = "test_silver_run_bronze"
    SILVER_TABLE = "test_silver_run_silver"

    @pytest.fixture(autouse=True)
    def _patch_uc_paths(self):
        orig_bronze = silver.BRONZE_UC_PATH
        orig_silver = silver.SILVER_UC_PATH
        silver.BRONZE_UC_PATH = self.BRONZE_TABLE
        silver.SILVER_UC_PATH = self.SILVER_TABLE
        yield
        silver.BRONZE_UC_PATH = orig_bronze
        silver.SILVER_UC_PATH = orig_silver

    def _create_bronze_data(self, spark, rows: list[dict]):
        """Populate the mock bronze table with test data."""
        spark.createDataFrame(rows).write.mode("overwrite").saveAsTable(self.BRONZE_TABLE)

    def test_creates_silver_table_and_writes_enriched_data(self, spark):
        self._create_bronze_data(
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
                },
            ],
        )

        silver.run(spark, geolite2_db=None)

        df = spark.table(self.SILVER_TABLE)
        assert df.count() == 1
        row = df.collect()[0]

        # Geo fields should be "Unknown" without a GeoIP DB
        assert row.country == "Unknown"
        assert row.region == "Unknown"
        assert row.city == "Unknown"

        # Computed fields
        assert row.page_category == "Homepage"  # uri_stem = "/"
        assert row.referrer_domain == "Direct"  # referrer = "-"
        assert row.traffic_type == "Direct"
        assert row.is_crawler == "false"  # IP not in crawler set

    def test_page_category_classification(self, spark):
        self._create_bronze_data(
            spark,
            [
                {
                    "log_date": "2009-10-24",
                    "log_time": "01:40:40",
                    "server_ip": "1.2.3.4",
                    "method": "GET",
                    "uri_stem": "/css/main.css",
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
                    "source_file": "test.log",
                },
                {
                    "log_date": "2009-10-24",
                    "log_time": "01:40:41",
                    "server_ip": "1.2.3.4",
                    "method": "GET",
                    "uri_stem": "/js/app.js",
                    "uri_query": "",
                    "client_ip": "1.2.3.4",
                    "user_agent": "UA",
                    "cookie": "",
                    "referrer": "-",
                    "status": 200,
                    "sub_status": 0,
                    "win32_status": 0,
                    "bytes_sent": 200,
                    "bytes_recv": 20,
                    "server_port": 80,
                    "username": "-",
                    "time_taken": 50,
                    "source_file": "test.log",
                },
                {
                    "log_date": "2009-10-24",
                    "log_time": "01:40:42",
                    "server_ip": "1.2.3.4",
                    "method": "GET",
                    "uri_stem": "/images/logo.png",
                    "uri_query": "",
                    "client_ip": "1.2.3.4",
                    "user_agent": "UA",
                    "cookie": "",
                    "referrer": "-",
                    "status": 200,
                    "sub_status": 0,
                    "win32_status": 0,
                    "bytes_sent": 300,
                    "bytes_recv": 30,
                    "server_port": 80,
                    "username": "-",
                    "time_taken": 50,
                    "source_file": "test.log",
                },
                {
                    "log_date": "2009-10-24",
                    "log_time": "01:40:43",
                    "server_ip": "1.2.3.4",
                    "method": "GET",
                    "uri_stem": "/report.pdf",
                    "uri_query": "",
                    "client_ip": "1.2.3.4",
                    "user_agent": "UA",
                    "cookie": "",
                    "referrer": "-",
                    "status": 200,
                    "sub_status": 0,
                    "win32_status": 0,
                    "bytes_sent": 400,
                    "bytes_recv": 40,
                    "server_port": 80,
                    "username": "-",
                    "time_taken": 50,
                    "source_file": "test.log",
                },
            ],
        )

        silver.run(spark, geolite2_db=None)

        df = spark.table(self.SILVER_TABLE)
        cats = {r.page_category for r in df.collect()}
        assert "Stylesheet" in cats
        assert "JavaScript" in cats
        assert "Image" in cats
        assert "Document" in cats

    def test_size_band_classification(self, spark):
        self._create_bronze_data(
            spark,
            [
                {
                    "log_date": "2009-10-24",
                    "log_time": "01:40:40",
                    "server_ip": "1.2.3.4",
                    "method": "GET",
                    "uri_stem": "/zero",
                    "uri_query": "",
                    "client_ip": "1.2.3.4",
                    "user_agent": "UA",
                    "cookie": "",
                    "referrer": "-",
                    "status": 200,
                    "sub_status": 0,
                    "win32_status": 0,
                    "bytes_sent": 0,
                    "bytes_recv": 0,
                    "server_port": 80,
                    "username": "-",
                    "time_taken": 50,
                    "source_file": "test.log",
                },
                {
                    "log_date": "2009-10-24",
                    "log_time": "01:40:41",
                    "server_ip": "1.2.3.4",
                    "method": "GET",
                    "uri_stem": "/tiny",
                    "uri_query": "",
                    "client_ip": "1.2.3.4",
                    "user_agent": "UA",
                    "cookie": "",
                    "referrer": "-",
                    "status": 200,
                    "sub_status": 0,
                    "win32_status": 0,
                    "bytes_sent": 500,
                    "bytes_recv": 100,
                    "server_port": 80,
                    "username": "-",
                    "time_taken": 50,
                    "source_file": "test.log",
                },
                {
                    "log_date": "2009-10-24",
                    "log_time": "01:40:42",
                    "server_ip": "1.2.3.4",
                    "method": "GET",
                    "uri_stem": "/small",
                    "uri_query": "",
                    "client_ip": "1.2.3.4",
                    "user_agent": "UA",
                    "cookie": "",
                    "referrer": "-",
                    "status": 200,
                    "sub_status": 0,
                    "win32_status": 0,
                    "bytes_sent": 5000,
                    "bytes_recv": 1000,
                    "server_port": 80,
                    "username": "-",
                    "time_taken": 50,
                    "source_file": "test.log",
                },
                {
                    "log_date": "2009-10-24",
                    "log_time": "01:40:43",
                    "server_ip": "1.2.3.4",
                    "method": "GET",
                    "uri_stem": "/large",
                    "uri_query": "",
                    "client_ip": "1.2.3.4",
                    "user_agent": "UA",
                    "cookie": "",
                    "referrer": "-",
                    "status": 200,
                    "sub_status": 0,
                    "win32_status": 0,
                    "bytes_sent": 500000,
                    "bytes_recv": 100000,
                    "server_port": 80,
                    "username": "-",
                    "time_taken": 50,
                    "source_file": "test.log",
                },
            ],
        )

        silver.run(spark, geolite2_db=None)

        df = spark.table(self.SILVER_TABLE)
        bands = {}
        for r in df.collect():
            bands[r.uri_stem] = r.size_band
        assert bands["/zero"] == "Zero"
        assert bands["/tiny"] == "Tiny (<1KB)"
        assert bands["/small"] == "Small (1-10KB)"
        assert bands["/large"] == "Large (>100KB)"

    def test_referrer_classification(self, spark):
        self._create_bronze_data(
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
                    "source_file": "test.log",
                },
                {
                    "log_date": "2009-10-24",
                    "log_time": "01:40:41",
                    "server_ip": "1.2.3.4",
                    "method": "GET",
                    "uri_stem": "/search",
                    "uri_query": "",
                    "client_ip": "1.2.3.4",
                    "user_agent": "UA",
                    "cookie": "",
                    "referrer": "http://google.com/search?q=test",
                    "status": 200,
                    "sub_status": 0,
                    "win32_status": 0,
                    "bytes_sent": 200,
                    "bytes_recv": 20,
                    "server_port": 80,
                    "username": "-",
                    "time_taken": 50,
                    "source_file": "test.log",
                },
                {
                    "log_date": "2009-10-24",
                    "log_time": "01:40:42",
                    "server_ip": "1.2.3.4",
                    "method": "GET",
                    "uri_stem": "/social",
                    "uri_query": "",
                    "client_ip": "1.2.3.4",
                    "user_agent": "UA",
                    "cookie": "",
                    "referrer": "http://facebook.com/post/123",
                    "status": 200,
                    "sub_status": 0,
                    "win32_status": 0,
                    "bytes_sent": 300,
                    "bytes_recv": 30,
                    "server_port": 80,
                    "username": "-",
                    "time_taken": 50,
                    "source_file": "test.log",
                },
                {
                    "log_date": "2009-10-24",
                    "log_time": "01:40:43",
                    "server_ip": "1.2.3.4",
                    "method": "GET",
                    "uri_stem": "/referral",
                    "uri_query": "",
                    "client_ip": "1.2.3.4",
                    "user_agent": "UA",
                    "cookie": "",
                    "referrer": "http://example.org/page",
                    "status": 200,
                    "sub_status": 0,
                    "win32_status": 0,
                    "bytes_sent": 400,
                    "bytes_recv": 40,
                    "server_port": 80,
                    "username": "-",
                    "time_taken": 50,
                    "source_file": "test.log",
                },
            ],
        )

        silver.run(spark, geolite2_db=None)

        df = spark.table(self.SILVER_TABLE)
        refs = {}
        for r in df.collect():
            refs[r.uri_stem] = (r.referrer_domain, r.traffic_type)
        assert refs["/"] == ("Direct", "Direct")
        assert refs["/search"] == ("google.com", "Search")
        assert refs["/social"] == ("facebook.com", "Social")
        assert refs["/referral"] == ("example.org", "Referral")

    def test_incremental_skip_already_enriched(self, spark):
        """Files in silver are skipped on re-run (source_file dedup)."""
        self._create_bronze_data(
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
                    "source_file": "unique.log",
                },
            ],
        )

        # First run — enriches and writes
        silver.run(spark, geolite2_db=None)
        assert spark.table(self.SILVER_TABLE).count() == 1

        # Second run — should skip (file already in silver)
        silver.run(spark, geolite2_db=None)
        assert spark.table(self.SILVER_TABLE).count() == 1
