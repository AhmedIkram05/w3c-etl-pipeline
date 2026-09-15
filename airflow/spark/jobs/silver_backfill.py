"""
Silver Range Backfill — reprocess a Bronze date range into Silver.

Reads the Bronze Delta table for ``log_date BETWEEN start_date AND end_date``,
re-applies the full Silver enrichment (GeoIP, User-Agent, computed fields),
and overwrites only those Silver partitions via Delta ``replaceWhere``.

Skew handling: crawler IPs are collected as a small distinct DataFrame and
broadcast-joined (no frozenset UDF), and rows are salted with
``floor(rand() * salt_buckets)`` then repartitioned across
``(target_partitions, log_date, salt)`` before the UDF chain runs.

Usage (via ``spark-submit``)::

    spark-submit silver_backfill.py \\
        --start-date 2009-10-24 --end-date 2009-10-26 \\
        --delta-dir /opt/spark/delta

Environment Variables
---------------------
``GEOIP_DB_PATH``
    Path to the GeoLite2-City.mmdb database. Used as the default for the
    ``--geolite2-db`` argument when the flag is not provided.
``GEOIP_ASN_DB_PATH``
    Path to the GeoLite2-ASN.mmdb database. Used as the default for the
    ``--geolite2-asn-db`` argument when the flag is not provided.
"""

import argparse
import datetime
import logging
import os
import statistics

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import broadcast, col, floor, lit, rand, when

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("silver_backfill")


def create_spark_session(app_name: str = "W3C_Silver_Backfill") -> SparkSession:
    """Build a SparkSession with Delta Lake support plus skew-mitigation extras."""
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
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.adaptive.skewedPartitionFactor", "5")
        .config("spark.sql.adaptive.skewedPartitionThresholdInBytes", "268435456")
        .config("spark.sql.autoBroadcastJoinThreshold", "33554432")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        .getOrCreate()
    )


def discover_crawler_ips_df(spark: SparkSession, bronze_path: str) -> DataFrame:
    """Return distinct ``client_ip`` values that requested ``robots.txt``."""
    try:
        return (
            spark.read
            .format("delta")
            .load(bronze_path)
            .filter(col("uri_stem").rlike("(?i).*robots\\.txt.*"))
            .filter(col("client_ip").isNotNull())
            .select("client_ip")
            .distinct()
        )
    except Exception:
        log.warning("Could not scan Bronze for crawler IPs; defaulting to empty.")
        return spark.createDataFrame([], "client_ip STRING")


def partition_stats(counts: list) -> dict:
    """Summarise driver-collected per-group counts as max/min/stddev/ratio."""
    counts = [int(c) for c in counts]
    if not counts:
        return {"max": 0, "min": 0, "stddev": 0.0, "ratio": 0.0, "groups": 0}
    mx, mn = max(counts), min(counts)
    return {
        "max": mx,
        "min": mn,
        "stddev": float(statistics.stdev(counts)) if len(counts) > 1 else 0.0,
        "ratio": float(mx / mn) if mn else float(mx),
        "groups": len(counts),
    }


def run(
    spark: SparkSession,
    start_date: str,
    end_date: str,
    delta_dir: str = "/opt/spark/delta",
    geolite2_db: str | None = None,
    geolite2_asn_db: str | None = None,
    salt_buckets: int = 16,
    target_partitions: int = 32,
    broadcast_threshold_mb: int = 32,
    repartition_cols: list | str | None = None,
    dry_run: bool = False,
):
    """Reprocess one Bronze date range into Silver via salted, broadcast-joined enrichment."""
    start = datetime.date.fromisoformat(start_date)
    end = datetime.date.fromisoformat(end_date)
    if start > end:
        raise ValueError(f"start_date {start_date} is after end_date {end_date}")

    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", f"{broadcast_threshold_mb}MB")
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    bronze_path = os.path.join(delta_dir, "bronze")
    silver_path = os.path.join(delta_dir, "silver")

    filtered = spark.read.format("delta").load(bronze_path).filter(col("log_date").between(start_date, end_date))
    rows_read = filtered.count()
    if rows_read == 0:
        log.warning("No Bronze rows in range %s..%s; nothing to backfill.", start_date, end_date)
        return {
            "rows_read": 0,
            "rows_written": 0,
            "partitions_before": partition_stats([]),
            "partitions_after": partition_stats([]),
            "partitions_before_ratio": 0.0,
            "partitions_after_ratio": 0.0,
            "broadcast_used": False,
        }

    before = partition_stats([r["count"] for r in filtered.groupBy("log_date").count().collect()])
    log.info("Range %s..%s: %d rows, partition skew ratio %.2f.", start_date, end_date, rows_read, before["ratio"])

    crawler_df = discover_crawler_ips_df(spark, bronze_path)
    broadcast_used = crawler_df.count() > 0
    log.info("Discovered crawler IPs via broadcast join (used=%s).", broadcast_used)
    if broadcast_used:
        flagged = (
            filtered
            .join(
                broadcast(crawler_df.withColumnRenamed("client_ip", "crawler_ip")),
                filtered.client_ip == col("crawler_ip"),
                "left",
            )
            .withColumn(
                "is_crawler",
                when(col("crawler_ip").isNotNull(), lit("true"))
                .when(col("client_ip").isNull() | col("client_ip").isin("-", "Unknown"), lit("Unknown"))
                .otherwise(lit("false")),
            )
            .drop("crawler_ip")
        )
    else:
        flagged = filtered.withColumn("is_crawler", lit("false"))

    cols = (
        repartition_cols.split(",") if isinstance(repartition_cols, str) else (repartition_cols or ["log_date", "salt"])
    )
    salted = flagged.withColumn("salt", floor(rand(42) * int(salt_buckets)).cast("int"))
    repart = salted.repartition(int(target_partitions), *[col(c) for c in cols])

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

    if geolite2_db:
        init_reader(geolite2_db)
    if geolite2_asn_db and os.path.exists(geolite2_asn_db):
        init_asn_reader(geolite2_asn_db)
    elif geolite2_asn_db:
        log.warning("GeoLite2-ASN DB not found at %s; isp will be Unknown.", geolite2_asn_db)
    from utils.transformations import page_category, referrer_domain, size_band, traffic_type
    from utils.ua_parser import (
        parse_agent_type,
        parse_browser_name,
        parse_browser_version,
        parse_device_type,
        parse_operating_system,
    )

    enriched = (
        repart
        .withColumn("country", geoip_country("client_ip"))
        .withColumn("region", geoip_region("client_ip"))
        .withColumn("city", geoip_city("client_ip"))
        .withColumn("latitude", geoip_latitude("client_ip"))
        .withColumn("longitude", geoip_longitude("client_ip"))
        .withColumn("postcode", geoip_postcode("client_ip"))
        .withColumn("isp", geoip_isp("client_ip"))
        .withColumn("agent_type", parse_agent_type("user_agent"))
        .withColumn("browser_name", parse_browser_name("user_agent"))
        .withColumn("browser_version", parse_browser_version("user_agent"))
        .withColumn("operating_system", parse_operating_system("user_agent"))
        .withColumn("device_type", parse_device_type("user_agent"))
        .withColumn("page_category", page_category("uri_stem"))
        .withColumn("referrer_domain", referrer_domain("referrer"))
        .withColumn("traffic_type", traffic_type("referrer"))
        .withColumn("size_band", size_band("bytes_sent", "bytes_recv"))
        .drop("salt")
    )

    after = partition_stats([r["count"] for r in repart.groupBy("salt").count().collect()])
    log.info("Post-salt bucket ratio %.2f (before %.2f).", after["ratio"], before["ratio"])

    if dry_run:
        log.info("Dry run: skipping Silver write for range %s..%s.", start_date, end_date)
        return {
            "rows_read": rows_read,
            "rows_written": 0,
            "partitions_before": before,
            "partitions_after": after,
            "partitions_before_ratio": before["ratio"],
            "partitions_after_ratio": after["ratio"],
            "broadcast_used": broadcast_used,
        }

    replace_where = f"log_date BETWEEN '{start_date}' AND '{end_date}'"
    enriched.write.format("delta").mode("overwrite").partitionBy("log_date").option(
        "replaceWhere", replace_where
    ).option("path", silver_path).save()

    rows_written = (
        spark.read.format("delta").load(silver_path).filter(col("log_date").between(start_date, end_date)).count()
    )
    log.info("Backfilled %d rows into Silver for range %s..%s.", rows_written, start_date, end_date)
    return {
        "rows_read": rows_read,
        "rows_written": rows_written,
        "partitions_before": before,
        "partitions_after": after,
        "partitions_before_ratio": before["ratio"],
        "partitions_after_ratio": after["ratio"],
        "broadcast_used": broadcast_used,
    }


def main():
    parser = argparse.ArgumentParser(description="Silver range backfill: reprocess a Bronze date range into Silver")
    parser.add_argument("--start-date", required=True, help="Range start (ISO YYYY-MM-DD).")
    parser.add_argument("--end-date", required=True, help="Range end (ISO YYYY-MM-DD).")
    parser.add_argument(
        "--delta-dir", default="/opt/spark/delta", help="Root Delta Lake directory (contains bronze/ and silver/)."
    )
    parser.add_argument(
        "--geolite2-db",
        default=os.environ.get("GEOIP_DB_PATH", "/opt/spark/data/GeoLite2-City.mmdb"),
        help="Path to GeoLite2-City.mmdb (defaults to $GEOIP_DB_PATH).",
    )
    parser.add_argument(
        "--geolite2-asn-db",
        default=os.environ.get("GEOIP_ASN_DB_PATH", "/opt/spark/data/GeoLite2-ASN.mmdb"),
        help="Path to GeoLite2-ASN.mmdb (defaults to $GEOIP_ASN_DB_PATH).",
    )
    parser.add_argument("--salt-buckets", type=int, default=16, help="Salt values for skew mitigation.")
    parser.add_argument("--target-partitions", type=int, default=32, help="Repartition count before enrichment.")
    parser.add_argument("--broadcast-threshold-mb", type=int, default=32, help="autoBroadcastJoinThreshold in MB.")
    parser.add_argument("--repartition-cols", default="log_date,salt", help="Comma-separated repartition columns.")
    parser.add_argument("--dry-run", action="store_true", help="Compute stats without writing Silver.")
    args = parser.parse_args()

    if args.geolite2_db and not os.path.exists(args.geolite2_db):
        log.warning("GeoLite2 DB not found at %s; geo fields will be Unknown.", args.geolite2_db)
    if args.geolite2_asn_db and not os.path.exists(args.geolite2_asn_db):
        log.warning("GeoLite2-ASN DB not found at %s; isp will be Unknown.", args.geolite2_asn_db)

    spark = create_spark_session()
    try:
        run(
            spark,
            args.start_date,
            args.end_date,
            args.delta_dir,
            args.geolite2_db,
            args.geolite2_asn_db,
            args.salt_buckets,
            args.target_partitions,
            args.broadcast_threshold_mb,
            args.repartition_cols,
            args.dry_run,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
