# Databricks notebook source
dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configurations"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

race_years = spark.read.format("delta").load(f"{gold}/race_results")\
    .filter(f"file_date = '{v_file_date}'") \
    .select("race_year").distinct().collect()

# COMMAND ----------

race_years_list = [years['race_year'] for years in race_years]

# COMMAND ----------

from pyspark.sql.functions import col
race_results_df = spark.read.format("delta").load(f"{gold}/race_results")\
    .filter(col("race_year").isin(race_years_list))

# COMMAND ----------

from pyspark.sql.functions import sum,count, when, col

constructor_standing_df = race_results_df \
    .groupBy("race_year", "team") \
    .agg(sum("points").alias("total_points")
         ,count(when(col("position") == 1, True)).alias("wins")
        ) 

# COMMAND ----------

from pyspark.sql.functions import rank, desc, dense_rank
from pyspark.sql.window import Window

final_df = constructor_standing_df \
           .withColumn("rank", dense_rank().over(Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))))


# COMMAND ----------

# overwrite_partition(final_df, "f1_gold", "constructor_standings", "race_year")

# COMMAND ----------

merge_condition = "tgt.team = src.team AND tgt.race_year = src.race_year"
merge_data(final_df, "f1_gold", "constructor_standings", gold, "race_year", merge_condition)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_year, COUNT(1)
# MAGIC FROM f1_gold.constructor_standings
# MAGIC GROUP BY race_year
# MAGIC ORDER BY race_year DESC
# MAGIC LIMIT 10
