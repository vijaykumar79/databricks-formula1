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

lap_times_schema = StructType([
    StructField("raceId", IntegerType(), False),
    StructField("driverId", IntegerType(), True),
    StructField("lap", IntegerType(), True),
    StructField("postion", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("milliseconds", IntegerType(), True)
])

# COMMAND ----------

lap_times_df = spark.read.schema(lap_times_schema).csv(f"{bronze}/{v_file_date}/lap_times")

# COMMAND ----------

from pyspark.sql.functions import concat, col, lit, current_timestamp

# COMMAND ----------

lap_times_add_date_df = add_ingestion_date(lap_times_df)

# COMMAND ----------

lap_times_final_df = lap_times_add_date_df.withColumnRenamed("raceId", "race_id") \
                                .withColumnRenamed("driverId", "driver_id") \
                                .withColumn("data_source", lit(v_data_source)) \
                                .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# overwrite_partition(lap_times_final_df, "f1_silver", "lap_times", "race_id")
merge_condition = "tgt.driver_id = src.driver_id AND tgt.race_id = src.race_id"
merge_data(lap_times_final_df, "f1_silver", "lap_times", silver, "race_id", merge_condition)

# COMMAND ----------

dbutils.notebook.exit("success")
