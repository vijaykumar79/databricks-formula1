# Databricks notebook source
dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configurations"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

results_year = spark.read.format("delta").load(f"{gold}/race_results") \
    .filter(f"file_date = '{v_file_date}'") \
    .select("race_year").distinct().collect()

# COMMAND ----------

race_year_list = [year["race_year"] for year in results_year]

# COMMAND ----------

from pyspark.sql.functions import col
race_results_df = spark.read.format("delta").load(f"{gold}/race_results") \
    .filter(col("race_year").isin(race_year_list))

# COMMAND ----------

from pyspark.sql.functions import sum,count, when, col

driver_standing_df = race_results_df \
    .groupBy("race_year", "driver_name", "driver_nationality", "team") \
    .agg(sum("points").alias("total_points")
         ,count(when(col("position") == 1, True)).alias("wins")
        ) 

# COMMAND ----------

from pyspark.sql.functions import rank, desc, dense_rank
from pyspark.sql.window import Window

final_df = driver_standing_df \
           .withColumn("rank", dense_rank().over(Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))))


# COMMAND ----------

# overwrite_partition(final_df, "f1_gold", "driver_standings", "race_year")

# COMMAND ----------

merge_condition = "tgt.driver_name = src.driver_name AND tgt.race_year = src.race_year"
merge_data(final_df, "f1_gold", "driver_standings", gold, "race_year", merge_condition)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_year, COUNT(1)
# MAGIC FROM f1_gold.driver_standings
# MAGIC GROUP BY race_year
# MAGIC ORDER BY race_year DESC
# MAGIC LIMIT 10
