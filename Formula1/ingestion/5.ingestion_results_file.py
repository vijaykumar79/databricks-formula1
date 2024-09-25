# Databricks notebook source
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configurations"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import IntegerType, StringType, StructType, StructField, DateType, DoubleType, FloatType 

# COMMAND ----------

results_schema = StructType([
    StructField("resultId", IntegerType(), False),
    StructField("raceId", IntegerType(), True),
    StructField("statusId", IntegerType(), True),
    StructField("constructorId", IntegerType(), True),
    StructField("driverId", IntegerType(), True),
    StructField("grid", IntegerType(), True),
    StructField("laps", IntegerType(), True),
    StructField("positionOrder", IntegerType(), True),
    StructField("fastestLap", IntegerType(), False),
    StructField("fastestLapSpeed", FloatType(), False),
    StructField("fastestLapTime", StringType(), False),
    StructField("milliseconds", IntegerType(), False),
    StructField("number", IntegerType(), False),
    StructField("position", IntegerType(), False),
    StructField("positionText", StringType(), False),
    StructField("rank", IntegerType(), False),
    StructField("points", FloatType(), False),
    StructField("time", StringType(), False)
])

# COMMAND ----------

from pyspark.sql.functions import concat, col, lit, current_timestamp

# COMMAND ----------

results_df = spark.read.schema(results_schema).json(f"{bronze}/{v_file_date}/results.json")

# COMMAND ----------

results_add_date_df = add_ingestion_date(results_df)

# COMMAND ----------

results_withcolumn_df = results_add_date_df.withColumnRenamed("resultId", "result_id") \
                                .withColumnRenamed("raceId", "race_id") \
                                .withColumnRenamed("constructorId", "constructor_id") \
                                .withColumnRenamed("driverId", "driver_id") \
                                .withColumnRenamed("positionOrder", "position_order") \
                                .withColumnRenamed("fastestLap", "fastest_lap") \
                                .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed") \
                                .withColumnRenamed("fastestLapTime", "fastest_lap_time") \
                                .withColumnRenamed("positionText", "position_text") \
                                .withColumn("data_source", lit(v_data_source)) \
                                .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

results_final_df = results_withcolumn_df.drop(col("statusId"))

# COMMAND ----------

# for race_id_list in results_final_df.select("race_id").distinct().collect():
#     if spark.catalog.tableExists("f1_silver.results"):
#         spark.sql(f"ALTER TABLE f1_silver.results DROP IF EXISTS PARTITION(race_id = {race_id_list.race_id})")

# COMMAND ----------

# results_final_df.write.mode("append").partitionBy("race_id").format("parquet").saveAsTable("f1_silver.results")

# COMMAND ----------

# overwrite_partition(results_final_df, "f1_silver", "results", "race_id")

# COMMAND ----------

merge_condition = "tgt.result_id = src.result_id AND tgt.race_id = src.race_id"

# COMMAND ----------

merge_data(results_final_df, "f1_silver", "results", silver, "race_id", merge_condition)

# COMMAND ----------

dbutils.notebook.exit("success")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id 
# MAGIC       ,COUNT(1) AS total_record
# MAGIC FROM f1_silver.results
# MAGIC GROUP BY race_id 
# MAGIC ORDER BY race_id DESC
