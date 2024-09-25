# Databricks notebook source
dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configurations"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

races_df = spark.read.format("delta").load(f"{silver}/races") \
    .select("race_id", "cirecuit_id", "name", "race_year", "race_timestamp") \
    .withColumnRenamed("race_timestamp", "race_date") \
    .withColumnRenamed("name", "race_name") \
    .withColumnRenamed("cirecuit_id", "circuit_id")

# COMMAND ----------

drivers_df = spark.read.format("delta").load(f"{silver}/drivers") \
    .select("driver_id", "name", "number", "nationality") \
    .withColumnRenamed("number", "driver_number") \
    .withColumnRenamed("nationality", "driver_nationality") \
    .withColumnRenamed("name", "driver_name")

# COMMAND ----------

circuits_df = spark.read.format("delta").load(f"{silver}/circuits") \
    .select("circuit_id", "location") \
    .withColumnRenamed("location", "circuit_location")

# COMMAND ----------

constructors_df = spark.read.format("delta").load(f"{silver}/constructors") \
    .select("constructor_id", "name") \
    .withColumnRenamed("name", "team")

# COMMAND ----------

results_df = spark.read.format("delta").load(f"{silver}/results") \
    .filter(f"file_date == '{v_file_date}'") \
    .select("result_id", "constructor_id", "driver_id", "race_id", "grid", "fastest_lap", "points", "time", "position") \
    .withColumnRenamed("time", "race_time") \
    .withColumnRenamed("race_id", "result_race_id")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

race_result_df = results_df.join(races_df, results_df.result_race_id == races_df.race_id, "inner") \
                           .join(drivers_df, drivers_df.driver_id == results_df.driver_id, "inner") \
                           .join(constructors_df, constructors_df.constructor_id == results_df.constructor_id, "inner") \
                           .join(circuits_df, circuits_df.circuit_id == races_df.circuit_id, "inner") \
                           .select(  races_df.race_year, races_df.race_name, races_df.race_date, circuits_df.circuit_location
                                    ,drivers_df.driver_name, drivers_df.driver_number, drivers_df.driver_nationality
                                    ,constructors_df.team, results_df.grid, results_df.fastest_lap, results_df.race_time 
                                    ,results_df.points,results_df.position
                                    ,results_df.result_race_id
                                   ) \
                           .withColumn("created_date", current_timestamp())  \
                           .withColumn("file_date", lit(v_file_date))  \
                           .withColumnRenamed("result_race_id", "race_id") 
                        

# COMMAND ----------

# overwrite_partition(race_result_df, "f1_gold", "race_results", "race_id")

# COMMAND ----------

merge_condition = "tgt.driver_name = src.driver_name AND tgt.race_id = src.race_id"
merge_data(race_result_df, "f1_gold", "race_results", gold, "race_id", merge_condition)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, COUNT(1)
# MAGIC FROM f1_gold.race_results
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC
# MAGIC LIMIT 10
