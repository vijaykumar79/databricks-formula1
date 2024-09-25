# Databricks notebook source
# MAGIC %run "../includes/configurations"

# COMMAND ----------

demo_df = spark.read.parquet(f"{gold}/race_results")

# COMMAND ----------

from pyspark.sql.functions import sum, countDistinct, count

# COMMAND ----------

demo_df \
    .groupBy("driver_name") \
    .agg(sum("points").alias("total_points"),countDistinct("race_name").alias("number_of_races")) \
    .orderBy("total_points", ascending = False) \
    .show()

# COMMAND ----------


