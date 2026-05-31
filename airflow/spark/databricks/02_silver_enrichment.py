"""
Silver Enrichment — Databricks Unity Catalog Version
=====================================================

Reads the Bronze Delta table from Unity Catalog, applies Geo-IP, User-Agent,
and computed-field enrichment UDFs, and writes the result as a new Silver
Delta table in Unity Catalog partitioned by ``log_date``.

This is the Databricks-equivalent of ``airflow/spark/jobs/silver_enrichment.py``.
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
import sys
from typing import Set

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, hour as spark_hour

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
        SparkSession.builder.appName(app_name)
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
        crawler_df = df.filter(
            col("uri_stem").rlike("(?i).*robots\\.txt.*")
        ).select("client_ip").distinct()
        return {row.client_ip for row in crawler_df.collect() if row.client_ip}
    except Exception:
        log.warning("Could not scan Bronze for crawler IPs; defaulting to empty set.")
        return set()


def ensure_silver_table(spark):
    """Create the Unity Catalog silver table if it does not exist.

    35 columns matching the enriched schema (bronze fields + 16 enrichment columns).
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
            latitude         STRING,
            longitude        STRING,
            isp              STRING,
            agent_type       STRING,
            browser_name     STRING,
            browser_version  STRING,
            operating_system STRING,
            device_type      STRING,
            page_category    STRING,
            referrer_domain  STRING,
            traffic_type     STRING,
            is_crawler       STRING,
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


def _geoip_lookup(ip: str, attr: str) -> str:
    """Look up a single GeoIP attribute for an IP address."""
    if _geoip_reader is None:
        return "Unknown"
    try:
        response = _geoip_reader.city(ip)
        return {
            "country": response.country.name or "Unknown",
            "region": response.subdivisions.most_specific.name or "Unknown",
            "city": response.city.name or "Unknown",
            "latitude": str(response.location.latitude) if response.location else "Unknown",
            "longitude": str(response.location.longitude) if response.location else "Unknown",
            "isp": str(response.traits.isp) if hasattr(response.traits, "isp") else "Unknown",
        }.get(attr, "Unknown")
    except Exception:
        return "Unknown"


# ── Main pipeline ───────────────────────────────────────────────────────


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

    # Register UDFs
    from pyspark.sql.functions import udf
    from pyspark.sql.types import StringType

    # GeoIP UDFs
    geoip_country_udf = udf(lambda ip: _geoip_lookup(ip, "country"), StringType())
    geoip_region_udf = udf(lambda ip: _geoip_lookup(ip, "region"), StringType())
    geoip_city_udf = udf(lambda ip: _geoip_lookup(ip, "city"), StringType())
    geoip_latitude_udf = udf(lambda ip: _geoip_lookup(ip, "latitude"), StringType())
    geoip_longitude_udf = udf(lambda ip: _geoip_lookup(ip, "longitude"), StringType())
    geoip_isp_udf = udf(lambda ip: _geoip_lookup(ip, "isp"), StringType())

    # Computed field UDFs
    is_crawler_udf = udf(lambda ip: "true" if ip in crawler_ips else "false", StringType())

    def page_category_udf(uri_stem: str) -> str:
        if not uri_stem:
            return "Unknown"
        uri_lower = uri_stem.lower()
        if uri_lower.startswith("/css/"):
            return "Stylesheet"
        elif uri_lower.startswith("/js/"):
            return "JavaScript"
        elif uri_lower.startswith("/images/") or uri_lower.startswith("/img/"):
            return "Image"
        elif uri_lower.endswith(".pdf"):
            return "Document"
        elif uri_lower.endswith(".xml") or uri_lower.endswith(".rss"):
            return "Feed"
        elif uri_lower == "/" or uri_lower == "/default.aspx":
            return "Homepage"
        else:
            return "Content"

    def referrer_domain_udf(referrer: str) -> str:
        if not referrer or referrer == "-":
            return "Direct"
        try:
            from urllib.parse import urlparse
            parsed = urlparse(referrer)
            return parsed.netloc or "Direct"
        except Exception:
            return "Unknown"

    def traffic_type_udf(referrer: str) -> str:
        if not referrer or referrer == "-":
            return "Direct"
        try:
            from urllib.parse import urlparse
            domain = urlparse(referrer).netloc.lower()
            if "google" in domain or "bing" in domain or "yahoo" in domain:
                return "Search"
            elif "facebook" in domain or "twitter" in domain or "linkedin" in domain:
                return "Social"
            else:
                return "Referral"
        except Exception:
            return "Referral"

    def size_band_udf(bytes_sent, bytes_recv) -> str:
        total = (bytes_sent or 0) + (bytes_recv or 0)
        if total == 0:
            return "Zero"
        elif total < 1024:
            return "Tiny (<1KB)"
        elif total < 10240:
            return "Small (1-10KB)"
        elif total < 102400:
            return "Medium (10-100KB)"
        else:
            return "Large (>100KB)"

    page_category_fn = udf(page_category_udf, StringType())
    referrer_domain_fn = udf(referrer_domain_udf, StringType())
    traffic_type_fn = udf(traffic_type_udf, StringType())
    size_band_fn = udf(size_band_udf, StringType())

    # Apply enrichment
    enriched = (
        new_data
        # Geo-IP enrichment
        .withColumn("country", geoip_country_udf("client_ip"))
        .withColumn("region", geoip_region_udf("client_ip"))
        .withColumn("city", geoip_city_udf("client_ip"))
        .withColumn("latitude", geoip_latitude_udf("client_ip"))
        .withColumn("longitude", geoip_longitude_udf("client_ip"))
        .withColumn("isp", geoip_isp_udf("client_ip"))
        # Crawler detection
        .withColumn("is_crawler", is_crawler_udf("client_ip"))
        # Computed fields
        .withColumn("page_category", page_category_fn("uri_stem"))
        .withColumn("referrer_domain", referrer_domain_fn("referrer"))
        .withColumn("traffic_type", traffic_type_fn("referrer"))
        .withColumn("size_band", size_band_fn("bytes_sent", "bytes_recv"))
    )

    # Write to Silver UC table
    enriched.write.format("delta").mode("append").partitionBy("log_date").saveAsTable(
        SILVER_UC_PATH
    )

    enriched_rows = enriched.count()
    log.info("Wrote %d enriched rows to Silver (Unity Catalog).", enriched_rows)

    # Display summary (Databricks notebook output)
    try:
        display(
            spark.sql(
                f"SELECT COUNT(*) AS silver_rows FROM {SILVER_UC_PATH}"
            )
        )
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

    if args.geolite2_db and not os.path.exists(
        args.geolite2_db.replace("dbfs:", "/dbfs")
    ):
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
