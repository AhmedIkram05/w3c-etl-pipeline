# Databricks notebook source
"""dbt Run — Azure SQL target"""

import dbt_common
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("dbt Run").getOrCreate()

tmpdir = dbt_common.bootstrap("dbt_run")
dbt_common.run_dbt_command(["dbt", "run"], tmpdir, "run")
dbt_common.cleanup(tmpdir)

print("dbt run completed successfully")
spark.stop()
