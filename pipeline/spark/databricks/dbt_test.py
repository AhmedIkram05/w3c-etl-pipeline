# Databricks notebook source
"""dbt Test — Azure SQL target"""

import dbt_common
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("dbt Test").getOrCreate()

tmpdir = dbt_common.bootstrap("dbt_test")
dbt_common.run_dbt_command(["dbt", "test"], tmpdir, "test")
dbt_common.cleanup(tmpdir)

print("dbt test completed successfully")
spark.stop()
