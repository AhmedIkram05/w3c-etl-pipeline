"""
Silver Enrichment — Bronze -> Silver Medallion Layer.

Reads the Bronze Delta table, applies Geo-IP, User-Agent, and computed-field
enrichment UDFs, and writes the result as a new Silver Delta table partitioned
by ``log_date``.

Usage (via ``spark-submit``)::

    spark-submit silver_enrichment.py \\
        --delta-dir /opt/spark/delta \\
        --geolite2-db /opt/spark/data/GeoLite2-City.mmdb \\
        --geolite2-asn-db /opt/spark/data/GeoLite2-ASN.mmdb

Environment Variables
---------------------
``GEOIP_DB_PATH``
    Path to the GeoLite2-City.mmdb database. Used as the default for the
    ``--geolite2-db`` argument when the flag is not provided. If neither the
    env var nor the flag is set, geo fields silently default to ``"Unknown"``
    and a warning is logged.

``GEOIP_ASN_DB_PATH``
    Path to the GeoLite2-ASN.mmdb database. Used as the default for the
    ``--geolite2-asn-db`` argument when the flag is not provided. The ASN
    database provides the ``isp`` / organisation field (City DB does not
    include ISP data). If neither the env var nor the flag is set, or the
    file does not exist on disk, the ``isp`` column will be ``"Unknown"`` and
    a warning is logged.

GeoLite2 Database
-----------------
The pipeline expects two MaxMind GeoLite2 ``.mmdb`` files (binary gzipped
DB) to be present at the paths configured by ``GEOIP_DB_PATH`` and
``GEOIP_ASN_DB_PATH`` (defaults ``/opt/spark/data/GeoLite2-City.mmdb`` and
``/opt/spark/data/GeoLite2-ASN.mmdb``). Neither file is shipped with this
repository.

To obtain them:
  1. Register for a free account at
     https://www.maxmind.com/en/geolite2/signup
  2. Download the **GeoLite2 City** and **GeoLite2 ASN** databases
     (binary ``.mmdb`` format — the files are named ``GeoLite2-City.mmdb``
     and ``GeoLite2-ASN.mmdb``).
  3. Mount/copy them into the Spark container at the paths referenced by
     ``GEOIP_DB_PATH`` / ``GEOIP_ASN_DB_PATH`` (or pass the corresponding
     CLI flags to override per-invocation).

If a file is missing at runtime, the corresponding geo fields (``country``,
``region``, ``city``, ``latitude``, ``longitude``, ``postcode``, ``isp``)
will be ``"Unknown"`` / ``None`` and a warning will be emitted. The
pipeline will still complete successfully.

Incremental: only processes ``source_file`` values that exist in Bronze but not
yet in Silver.  Idempotent: re-running appends only new source files.
"""

import argparse
import logging
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("silver_enrichment")


def create_spark_session(app_name: str = "W3C_Silver_Enrichment") -> SparkSession:
    """Build a SparkSession with Delta Lake support."""
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
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        .getOrCreate()
    )


def get_source_files_in_delta(spark: SparkSession, delta_path: str) -> set:
    """Return the set of ``source_file`` values already in a Delta table.

    Returns an empty set if the table does not exist yet.
    """
    try:
        df = spark.read.format("delta").load(delta_path)
        rows = df.select("source_file").distinct().collect()
        return {row.source_file for row in rows}
    except Exception:
        return set()


def discover_crawler_ips(spark: SparkSession, delta_dir: str) -> set:
    """Identify IPs that requested ``robots.txt`` — these are crawlers.

    Scans the Bronze Delta table for URI stems containing ``robots.txt``
    and returns the distinct client IPs found.
    """
    bronze_path = os.path.join(delta_dir, "bronze")
    try:
        df = spark.read.format("delta").load(bronze_path)
        crawler_df = df.filter(col("uri_stem").rlike("(?i).*robots\\.txt.*")).select("client_ip").distinct()
        return {row.client_ip for row in crawler_df.collect() if row.client_ip}
    except Exception:
        log.warning("Could not scan Bronze for crawler IPs; defaulting to empty set.")
        return set()


def run(spark: SparkSession, delta_dir: str, geolite2_db: str | None = None, geolite2_asn_db: str | None = None):
    """Execute the Silver enrichment pipeline.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session.
    delta_dir : str
        Root directory containing ``bronze/`` and ``silver/`` Delta tables.
    geolite2_db : str or None
        Path to the GeoLite2-City.mmdb file.  If ``None``, GeoIP enrichment
        will produce ``"Unknown"`` for all city-derived geo fields.
    geolite2_asn_db : str or None
        Path to the GeoLite2-ASN.mmdb file.  If ``None`` or the file does
        not exist, the ``isp`` column will be ``"Unknown"`` for all rows.
    """
    bronze_path = os.path.join(delta_dir, "bronze")
    silver_path = os.path.join(delta_dir, "silver")

    # ── 1. Determine what's new since last run ───────────────────────
    bronze_loaded = get_source_files_in_delta(spark, bronze_path)
    silver_loaded = get_source_files_in_delta(spark, silver_path)

    new_files = bronze_loaded - silver_loaded

    if not new_files:
        log.info("No new source files to enrich. Silver is up-to-date.")
        return

    log.info("Found %d new source file(s) to enrich.", len(new_files))

    # ── 2. Read Bronze data for new files ────────────────────────────
    bronze_df: DataFrame = spark.read.format("delta").load(bronze_path)
    new_data = bronze_df.filter(col("source_file").isin(new_files))

    row_count = new_data.count()
    log.info("Read %d new rows from Bronze.", row_count)
    if row_count == 0:
        return

    # ── 3. Discover crawler IPs (from ALL Bronze data, not just new) ─
    crawler_ips = discover_crawler_ips(spark, delta_dir)
    log.info("Discovered %d crawler IPs (requested robots.txt).", len(crawler_ips))

    # ── 4. Apply enrichment UDFs ─────────────────────────────────────
    # Import here so the Spark worker can resolve them.
    from utils.geoip import (
        geoip_city,
        geoip_country,
        geoip_isp,
        geoip_latitude,
        geoip_longitude,
        geoip_postcode,
        geoip_region,
        init_asn_reader,
        init_reader,
    )

    # Initialise the GeoLite2 readers with the configured database paths
    # (overrides the module-level defaults in utils/geoip.py).
    if geolite2_db:
        init_reader(geolite2_db)
    if geolite2_asn_db and os.path.exists(geolite2_asn_db):
        init_asn_reader(geolite2_asn_db)
    elif geolite2_asn_db:
        log.warning("GeoLite2-ASN DB not found at %s; isp will be Unknown.", geolite2_asn_db)
    from utils.transformations import (
        make_crawler_udf,
        page_category,
        referrer_domain,
        size_band,
        traffic_type,
    )
    from utils.ua_parser import (
        parse_agent_type,
        parse_browser_name,
        parse_browser_version,
        parse_device_type,
        parse_operating_system,
    )

    is_crawler_udf = make_crawler_udf(crawler_ips)

    enriched = (
        new_data
        # Geo-IP enrichment
        .withColumn("country", geoip_country("client_ip"))
        .withColumn("region", geoip_region("client_ip"))
        .withColumn("city", geoip_city("client_ip"))
        .withColumn("latitude", geoip_latitude("client_ip"))
        .withColumn("longitude", geoip_longitude("client_ip"))
        .withColumn("postcode", geoip_postcode("client_ip"))
        .withColumn("isp", geoip_isp("client_ip"))
        # User-Agent parsing
        .withColumn("agent_type", parse_agent_type("user_agent"))
        .withColumn("browser_name", parse_browser_name("user_agent"))
        .withColumn("browser_version", parse_browser_version("user_agent"))
        .withColumn("operating_system", parse_operating_system("user_agent"))
        .withColumn("device_type", parse_device_type("user_agent"))
        # Computed fields
        .withColumn("page_category", page_category("uri_stem"))
        .withColumn("referrer_domain", referrer_domain("referrer"))
        .withColumn("traffic_type", traffic_type("referrer"))
        .withColumn("is_crawler", is_crawler_udf("client_ip"))
        .withColumn("size_band", size_band("bytes_sent", "bytes_recv"))
    )

    # ── 5. Write to Silver Delta table ───────────────────────────────
    enriched.write.format("delta").mode("append").partitionBy("log_date").option("path", silver_path).save()

    enriched_rows = enriched.count()
    log.info("Wrote %d enriched rows to Silver (Delta).", enriched_rows)


def main():
    parser = argparse.ArgumentParser(description="Silver Enrichment: Bronze → Silver (GeoIP + UA + computed fields)")
    parser.add_argument(
        "--delta-dir",
        default="/opt/spark/delta",
        help="Root Delta Lake directory (contains bronze/ and silver/).",
    )
    parser.add_argument(
        "--geolite2-db",
        default=os.environ.get("GEOIP_DB_PATH"),
        help=(
            "Path to GeoLite2-City.mmdb file. "
            "Defaults to the $GEOIP_DB_PATH environment variable. "
            "If neither is set, geo fields default to Unknown."
        ),
    )
    parser.add_argument(
        "--geolite2-asn-db",
        default=os.environ.get("GEOIP_ASN_DB_PATH", "/opt/spark/data/GeoLite2-ASN.mmdb"),
        help=(
            "Path to GeoLite2-ASN.mmdb file (used for the isp field). "
            "Defaults to the $GEOIP_ASN_DB_PATH environment variable, "
            "or /opt/spark/data/GeoLite2-ASN.mmdb. If the file does not "
            "exist at runtime, isp will be Unknown."
        ),
    )
    args = parser.parse_args()

    if args.geolite2_db and not os.path.exists(args.geolite2_db):
        log.warning("GeoLite2 DB not found at %s; geo fields will be Unknown.", args.geolite2_db)
    if args.geolite2_asn_db and not os.path.exists(args.geolite2_asn_db):
        log.warning("GeoLite2-ASN DB not found at %s; isp will be Unknown.", args.geolite2_asn_db)

    spark = create_spark_session()
    try:
        run(spark, args.delta_dir, args.geolite2_db, args.geolite2_asn_db)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
