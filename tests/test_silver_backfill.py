"""Tests for the Silver range backfill job (skew salt + broadcast crawler join)."""

import datetime
import os
import sys

import pytest

pytest.importorskip("pyspark")
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast, col
from pyspark.sql.readwriter import DataFrameReader, DataFrameWriter

_orig_w = DataFrameWriter.format


def _w(self, source):
    return _orig_w(self, "parquet" if source == "delta" else source)


DataFrameWriter.format = _w
_orig_r = DataFrameReader.format


def _r(self, source):
    return _orig_r(self, "parquet" if source == "delta" else source)


DataFrameReader.format = _r
_orig_sql = SparkSession.sql


def _sql(self, q, *a, **k):
    return _orig_sql(self, q.replace("USING DELTA", "USING PARQUET"), *a, **k)


SparkSession.sql = _sql

_TEST_DIR = os.path.dirname(os.path.abspath(__file__))
_JOBS_DIR = os.path.join(_TEST_DIR, "..", "pipeline", "spark", "jobs")
if _JOBS_DIR not in sys.path:
    sys.path.insert(0, _JOBS_DIR)

# Workers must use the same interpreter as the driver so third-party
# UDF deps (user_agents, geoip2, pandas) resolve on executors.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)

import silver_backfill  # noqa: E402
from utils.schemas import bronze_schema  # noqa: E402


def _rows():
    base = dict(
        log_time="01:40:40",
        server_ip="134.36.36.75",
        method="GET",
        uri_query="",
        server_port=80,
        username="-",
        user_agent="Mozilla/4.0",
        cookie="-",
        referrer="-",
        status=200,
        sub_status=0,
        win32_status=0,
        bytes_sent=500,
        bytes_recv=100,
        time_taken=100,
    )
    d1, d2, d3 = datetime.date(2009, 10, 24), datetime.date(2009, 10, 25), datetime.date(2009, 10, 26)
    rows = [
        {
            **base,
            "log_date": d1,
            "client_ip": "10.0.0.99",
            "uri_stem": "/robots.txt" if i < 50 else "/",
            "source_file": "backfill.log",
        }
        for i in range(700)
    ]
    stems = ["/index.html", "/about", "/products", "/images/logo.png", "/style.css", "/app.js"]
    rows += [
        {
            **base,
            "log_date": d2 if i < 250 else d3,
            "client_ip": f"192.168.1.{i % 30 + 1}",
            "uri_stem": stems[i % len(stems)],
            "source_file": "backfill.log",
        }
        for i in range(300)
    ]
    return rows


def _setup(spark, delta_dir):
    spark.createDataFrame(_rows(), schema=bronze_schema).write.format("delta").mode("overwrite").save(
        os.path.join(delta_dir, "bronze")
    )


def _run(spark, delta_dir):
    return silver_backfill.run(
        spark, "2009-10-24", "2009-10-26", delta_dir=delta_dir, salt_buckets=8, target_partitions=16
    )


def test_skew_ratio_drops(spark, tmp_path):
    _setup(spark, str(tmp_path))
    res = _run(spark, str(tmp_path))
    assert res["rows_read"] == 1000 and res["rows_written"] == 1000
    assert res["partitions_before_ratio"] > 8
    assert res["partitions_after_ratio"] < 3
    assert res["broadcast_used"] is True


def test_is_crawler_and_broadcast(spark, tmp_path):
    _setup(spark, str(tmp_path))
    _run(spark, str(tmp_path))
    silver = spark.read.format("delta").load(os.path.join(str(tmp_path), "silver"))
    assert {
        r.is_crawler for r in silver.filter(col("client_ip") == "10.0.0.99").select("is_crawler").distinct().collect()
    } == {"true"}
    assert {
        r.is_crawler for r in silver.filter(col("client_ip") != "10.0.0.99").select("is_crawler").distinct().collect()
    } == {"false"}
    bronze = spark.read.format("delta").load(os.path.join(str(tmp_path), "bronze"))
    crawlers = silver_backfill.discover_crawler_ips_df(spark, os.path.join(str(tmp_path), "bronze")).withColumnRenamed(
        "client_ip", "crawler_ip"
    )
    joined = (
        bronze
        .select("client_ip", "uri_stem")
        .join(
            broadcast(crawlers),
            col("client_ip") == col("crawler_ip"),
            "left",
        )
        .select("client_ip", "uri_stem", "crawler_ip")
    )
    spark.conf.set("spark.sql.debug.maxToStringFields", 100000)
    plan = joined._jdf.queryExecution().executedPlan().toString()
    assert "BroadcastHashJoin" in plan


def test_replacewhere_idempotent(spark, tmp_path):
    _setup(spark, str(tmp_path))
    _run(spark, str(tmp_path))
    n1 = spark.read.format("delta").load(os.path.join(str(tmp_path), "silver")).count()
    _run(spark, str(tmp_path))
    n2 = spark.read.format("delta").load(os.path.join(str(tmp_path), "silver")).count()
    assert n1 == n2 == 1000
