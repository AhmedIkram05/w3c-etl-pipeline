"""
Silver Enrichment — Databricks Unity Catalog Version
=====================================================

Reads the Bronze Delta table from Unity Catalog, applies Geo-IP, User-Agent,
and computed-field enrichment UDFs, and writes the result as a new Silver
Delta table in Unity Catalog partitioned by ``log_date``.

This is the Databricks-equivalent of ``pipeline/spark/jobs/silver_enrichment.py``.
It uses Unity Catalog paths instead of local Delta directories.

Usage
-----
Run as a Databricks Python notebook or script on a cluster with access to
the ``w3c_catalog`` Unity Catalog namespace::

    # Via Databricks CLI or workflow
    databricks jobs run-now --job-id <job-id> \\
        --jar-params '["--geolite2-db","dbfs:/mnt/geolite2/GeoLite2-City.mmdb"]'

Requirements
------------
- Unity Catalog tables: ``w3c_catalog.bronze.raw_logs`` and
  ``w3c_catalog.silver.enriched_logs``
- GeoLite2-City.mmdb in DBFS (optional — geo fields default to "Unknown")
- Cluster configured with Unity Catalog + Delta Lake support
- Databricks Runtime 13.3+ (for ``@pandas_udf`` and PyArrow support)
"""

import argparse
import logging
import os
from typing import Set

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import BooleanType, FloatType, StringType, StructField, StructType

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("databricks_silver")

# ── Unity Catalog paths ─────────────────────────────────────────────────
# Configurable — update for your workspace.
CATALOG = "w3c_catalog"
BRONZE_SCHEMA = "bronze"
BRONZE_TABLE = "raw_logs"
SILVER_SCHEMA = "silver"
SILVER_TABLE = "enriched_logs"

BRONZE_UC_PATH = f"{CATALOG}.{BRONZE_SCHEMA}.{BRONZE_TABLE}"
SILVER_UC_PATH = f"{CATALOG}.{SILVER_SCHEMA}.{SILVER_TABLE}"


# ── Helpers ─────────────────────────────────────────────────────────────


def create_spark_session(app_name: str = "databricks_silver") -> SparkSession:
    """Create or reuse a Spark session for Databricks."""
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )


def get_source_files_in_uc(spark, uc_path: str) -> Set[str]:
    """Return ``source_file`` values already in a Unity Catalog table.

    Returns an empty set if the table does not exist yet.
    """
    try:
        df = spark.table(uc_path)
        rows = df.select("source_file").distinct().collect()
        return {row.source_file for row in rows}
    except Exception:
        return set()


def discover_crawler_ips(spark) -> Set[str]:
    """Identify IPs that requested ``robots.txt`` from the bronze UC table."""
    try:
        df = spark.table(BRONZE_UC_PATH)
        crawler_df = df.filter(col("uri_stem").rlike("(?i).*robots\\.txt.*")).select("client_ip").distinct()
        return {row.client_ip for row in crawler_df.collect() if row.client_ip}
    except Exception:
        log.warning("Could not scan Bronze for crawler IPs; defaulting to empty set.")
        return set()


def ensure_silver_table(spark):
    """Create the Unity Catalog silver table if it does not exist.

    30 columns matching the enriched schema (19 bronze fields + 11 enrichment columns).
    """
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {SILVER_UC_PATH} (
            log_date         STRING,
            log_time         STRING,
            server_ip        STRING,
            method           STRING,
            uri_stem         STRING,
            uri_query        STRING,
            client_ip        STRING,
            user_agent       STRING,
            cookie           STRING,
            referrer         STRING,
            status           INT,
            sub_status       INT,
            win32_status     INT,
            bytes_sent       BIGINT,
            bytes_recv       BIGINT,
            server_port      INT,
            username         STRING,
            time_taken       BIGINT,
            source_file      STRING,
            country          STRING,
            region           STRING,
            city             STRING,
            latitude         FLOAT,
            longitude        FLOAT,
            isp              STRING,

            page_category    STRING,
            referrer_domain  STRING,
            traffic_type     STRING,
            is_crawler       BOOLEAN,
            size_band        STRING
        )
        USING DELTA
        PARTITIONED BY (log_date)
        """
    )
    log.info("Ensured target table %s exists.", SILVER_UC_PATH)


# ── Enrichment UDFs ─────────────────────────────────────────────────────
# Inline UDF wrappers for Databricks self-containment.
# In production, these would import from the shared utils/ package.
# For now, we provide stubs that delegate to inline implementations
# or return "Unknown" when the dependencies are not available.


def _init_geoip(db_path: str | None):
    """Initialise GeoIP reader. No-op if path is None or file missing."""
    global _geoip_reader
    _geoip_reader = None
    if db_path and os.path.exists(db_path.replace("dbfs:", "/dbfs")):
        try:
            from geoip2.database import Reader

            _geoip_reader = Reader(db_path.replace("dbfs:", "/dbfs"))
            log.info("GeoIP reader initialised from %s", db_path)
        except Exception as exc:
            log.warning("Failed to initialise GeoIP reader: %s", exc)


def _geo_lookup(ip: str) -> dict | None:
    """Single consolidated GeoIP lookup returning all six fields, or None on a miss.

    Contract shared with ``dlt_silver._geo_lookup``: a hit returns a dict, a
    miss (no reader, absent IP, or read error) returns ``None`` and the caller
    decides how to fill the gap. This path reads ``geoip2`` (whose City response
    carries ``isp`` but not ``postcode``); ``dlt_silver`` reads ``maxminddb``
    and carries ``postcode`` but takes ``isp`` from the ASN DB. The target
    schemas differ (UC silver has ``isp``, DLT silver has ``postcode``) and the
    two readers cannot be shared, so only the ``dict | None`` contract is
    unified.
    """
    if _geoip_reader is None:
        return None
    try:
        response = _geoip_reader.city(ip)
        return {
            "country": response.country.name or "Unknown",
            "region": response.subdivisions.most_specific.name or "Unknown",
            "city": response.city.name or "Unknown",
            "latitude": float(response.location.latitude) if response.location else None,
            "longitude": float(response.location.longitude) if response.location else None,
            "isp": str(response.traits.isp) if hasattr(response.traits, "isp") else "Unknown",
        }
    except Exception:
        return None


# ── Consolidated GeoIP struct UDF ────────────────────────────────────
# One mmdb call per row returns all six fields (was six scalar UDFs, each
# performing its own city() lookup per row).

_GEO_STRUCT_SCHEMA = StructType([
    StructField("country", StringType(), True),
    StructField("region", StringType(), True),
    StructField("city", StringType(), True),
    StructField("latitude", FloatType(), True),
    StructField("longitude", FloatType(), True),
    StructField("isp", StringType(), True),
])

# Unknown-struct defaults applied by the UDF when the lookup misses. Kept
# separate from _geo_lookup so the lookup keeps the shared dict | None contract.
_GEO_UNKNOWN = {
    "country": "Unknown",
    "region": "Unknown",
    "city": "Unknown",
    "latitude": None,
    "longitude": None,
    "isp": "Unknown",
}


@udf(returnType=_GEO_STRUCT_SCHEMA)
def get_geo_fields(ip: str) -> dict:
    """Consolidated GeoIP UDF wrapping :func:`_geo_lookup`.

    A miss (``None``) is filled with the canonical "Unknown" struct here so the
    UDF keeps emitting non-null text fields (coordinates stay NULL).
    """
    result = _geo_lookup(ip)
    return result if result is not None else dict(_GEO_UNKNOWN)


# ── Main pipeline ───────────────────────────────────────────────────────


# Canonical page-category vocabulary, mirroring utils/transformations
# (_EXTENSION_CATEGORIES) and the dbt dim_page model.
_EXTENSION_CATEGORIES = {
    "aspx": "Dynamic Page",
    "asp": "Dynamic Page",
    "html": "Static Page",
    "htm": "Static Page",
    "shtml": "Static Page",
    "jpg": "Image",
    "jpeg": "Image",
    "png": "Image",
    "gif": "Image",
    "bmp": "Image",
    "ico": "Icon",
    "css": "Stylesheet",
    "js": "Script",
    "txt": "Text File",
    "pdf": "Document",
    "no_extension": "Directory",
}


def _page_category(uri_stem: str) -> str:
    """Canonical page-category vocabulary.

    Mirrors utils/transformations.page_category and the dbt dim_page
    model: Dynamic Page, Static Page, Image, Icon, Stylesheet, Script,
    Text File, Document, Directory, Other; absent URI stems resolve to
    'Unknown'.
    """
    if not uri_stem or uri_stem.strip() in ("-", "Unknown", ""):
        return "Unknown"
    stem = uri_stem.split("?")[0]
    if "." in stem:
        ext = stem.rsplit(".", 1)[-1].lower().strip()
        return _EXTENSION_CATEGORIES.get(ext, "Other")
    return "Directory"


def _referrer_domain(referrer: str) -> str:
    """Canonical referrer-domain extraction.

    Mirrors utils/transformations.referrer_domain: absent (''/ '-')
    referrers -> 'Direct', the literal sentinel 'Unknown' -> 'Unknown',
    scheme-less URLs get a scheme prepended so the domain is still
    extracted (a real referrer must never be misclassified as direct
    traffic), and parse failures fall back to the raw input. Only a
    LEADING 'www.' label is stripped (``str.replace`` would also strip a
    mid-label 'www.', e.g. 'abcwww.com' -> 'abccom').
    """
    if not referrer or referrer.strip() == "-":
        return "Direct"
    if referrer == "Unknown":
        return "Unknown"
    try:
        from urllib.parse import urlparse

        candidate = referrer.strip()
        if "//" not in candidate:
            candidate = "http://" + candidate
        parsed = urlparse(candidate)
        host = parsed.netloc.split("@")[-1].split(":")[0].lower()
        domain = host[4:] if host.startswith("www.") else host
        return domain or referrer
    except Exception:
        return str(referrer)


# Canonical traffic-source vocabulary, mirroring utils/transformations and the
# dbt fact_webrequest.traffic_type accepted values (Search Engine, Social,
# Internal, Referral, Direct, Unknown).
_SEARCH_ENGINES = ["google", "bing", "yahoo", "baidu", "duckduckgo", "yandex", "ask"]
_SOCIAL_DOMAINS = ["facebook", "twitter", "linkedin", "reddit", "t.co", "instagram"]
_INTERNAL_DOMAINS = ["darwinsbeagleplants", "134.36.36.75"]


def _traffic_type(referrer: str) -> str:
    """Classify a referrer into the canonical traffic-source vocabulary.

    Mirrors utils/transformations.traffic_type and dlt_silver: Direct,
    Unknown, Internal, Search Engine, Social, Referral. Matching is on
    whole dot-delimited domain labels, so 'notgoogle.com' is not a search
    referrer.
    """
    if not referrer or referrer.strip() == "-":
        return "Direct"
    if referrer == "Unknown":
        return "Unknown"

    domain = _referrer_domain(referrer)
    padded = f".{domain}."
    if any(f".{s}." in padded for s in _INTERNAL_DOMAINS):
        return "Internal"
    if any(f".{s}." in padded for s in _SEARCH_ENGINES):
        return "Search Engine"
    if any(f".{s}." in padded for s in _SOCIAL_DOMAINS):
        return "Social"
    return "Referral"


def _size_band(bytes_sent, bytes_recv) -> str:
    """Canonical size bands over total bytes (sent + received).

    Mirrors utils/transformations.size_band: Zero, Tiny, Small, Medium,
    Large, Huge (thresholds 0 / 1KB / 10KB / 100KB / 1MB).
    """
    total = (bytes_sent or 0) + (bytes_recv or 0)
    if total == 0:
        return "Zero"
    if total < 1024:
        return "Tiny"
    if total < 10240:
        return "Small"
    if total < 102400:
        return "Medium"
    if total < 1048576:
        return "Large"
    return "Huge"


def make_crawler_udf(crawler_ips):
    """Boolean ``is_crawler`` UDF closed over the crawler-IP set.

    Mirrors utils/transformations.make_crawler_udf: BooleanType output (the
    canonical silver/warehouse contract) and an unusable IP is not a crawler.
    """
    ips = frozenset(crawler_ips)

    @udf(returnType=BooleanType())
    def is_crawler_impl(ip):
        return bool(ip) and ip.strip() not in ("-", "Unknown", "") and ip.strip() in ips

    return is_crawler_impl


def run(spark, geolite2_db: str | None = None):
    """Execute the Silver enrichment pipeline on Databricks."""
    # Ensure target table
    ensure_silver_table(spark)

    # Determine what's new
    bronze_loaded = get_source_files_in_uc(spark, BRONZE_UC_PATH)
    silver_loaded = get_source_files_in_uc(spark, SILVER_UC_PATH)
    new_files = bronze_loaded - silver_loaded

    if not new_files:
        log.info("No new source files to enrich. Silver is up-to-date.")
        return

    log.info("Found %d new source file(s) to enrich.", len(new_files))

    # Read bronze data for new files
    bronze_df = spark.table(BRONZE_UC_PATH)
    new_data = bronze_df.filter(col("source_file").isin(new_files))
    row_count = new_data.count()
    log.info("Read %d new rows from Bronze.", row_count)
    if row_count == 0:
        return

    # Discover crawler IPs
    crawler_ips = discover_crawler_ips(spark)
    log.info("Discovered %d crawler IPs (requested robots.txt).", len(crawler_ips))

    # Initialise GeoIP
    _init_geoip(geolite2_db)

    # Computed field UDFs
    is_crawler_udf = make_crawler_udf(crawler_ips)

    page_category_fn = udf(_page_category, StringType())
    referrer_domain_fn = udf(_referrer_domain, StringType())
    traffic_type_fn = udf(_traffic_type, StringType())
    size_band_fn = udf(_size_band, StringType())

    # Apply enrichment
    enriched = (
        new_data
        # Geo-IP enrichment: one mmdb call per row, fields unpacked from the struct
        .withColumn("geo", get_geo_fields("client_ip"))
        .withColumn("country", col("geo.country"))
        .withColumn("region", col("geo.region"))
        .withColumn("city", col("geo.city"))
        .withColumn("latitude", col("geo.latitude"))
        .withColumn("longitude", col("geo.longitude"))
        .withColumn("isp", col("geo.isp"))
        # Crawler detection
        .withColumn("is_crawler", is_crawler_udf("client_ip"))
        # Computed fields
        .withColumn("page_category", page_category_fn("uri_stem"))
        .withColumn("referrer_domain", referrer_domain_fn("referrer"))
        .withColumn("traffic_type", traffic_type_fn("referrer"))
        .withColumn("size_band", size_band_fn("bytes_sent", "bytes_recv"))
        .drop("geo")
    )

    # Write to Silver UC table
    # No post-write count(): on an uncached DataFrame it re-executes the
    # whole bronze scan + UDF pipeline just for a log line.
    enriched.write.format("delta").mode("append").partitionBy("log_date").saveAsTable(SILVER_UC_PATH)
    log.info("Silver enrichment write complete (Unity Catalog).")

    # Display summary (Databricks notebook output)
    try:
        # SILVER_UC_PATH is a module constant and not user input.
        display(spark.sql(f"SELECT COUNT(*) AS silver_rows FROM {SILVER_UC_PATH}"))  # type: ignore[name-defined]  # noqa: F821  # nosec B608
    except NameError:
        pass


def main():
    parser = argparse.ArgumentParser(
        description="Silver Enrichment (Databricks UC): Bronze → Silver (GeoIP + UA + computed fields)"
    )
    parser.add_argument(
        "--geolite2-db",
        default=None,
        help="DBFS path to GeoLite2-City.mmdb (e.g. dbfs:/mnt/geolite2/GeoLite2-City.mmdb).",
    )
    args = parser.parse_args()

    if args.geolite2_db and not os.path.exists(args.geolite2_db.replace("dbfs:", "/dbfs")):
        log.warning(
            "GeoLite2 DB not found at %s; geo fields will be Unknown.",
            args.geolite2_db,
        )

    spark = create_spark_session()
    try:
        run(spark, args.geolite2_db)
    finally:
        spark.stop()


_geoip_reader = None

if __name__ == "__main__":
    main()
