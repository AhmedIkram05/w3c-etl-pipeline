"""Shared Spark session + file-discovery helpers (canonical single source)."""

from pyspark.sql import SparkSession


def create_spark_session(app_name: str = "W3C_ETL") -> SparkSession:
    """Create a SparkSession with the shared W3C ETL config."""
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.ui.enabled", "false")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


def get_loaded_files(spark, path: str) -> set:
    """Return set of source_file values already present at ``path``."""
    try:
        df = spark.read.format("delta").load(path)
        return {r.source_file for r in df.select("source_file").distinct().collect() if r.source_file}
    except Exception:
        return set()
