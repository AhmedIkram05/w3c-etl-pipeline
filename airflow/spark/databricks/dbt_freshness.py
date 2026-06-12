# Databricks notebook source
"""dbt Source Freshness — Azure SQL target"""

import dbt_common
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("dbt Source Freshness").getOrCreate()

tmpdir = dbt_common.bootstrap("dbt_freshness")
dbt_common.run_dbt_command(["dbt", "source", "freshness"], tmpdir, "source freshness")
dbt_common.cleanup(tmpdir)

print("dbt source freshness completed")
spark.stop()
