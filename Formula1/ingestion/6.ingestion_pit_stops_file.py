# Databricks notebook source
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %run "../includes/configurations"

# COMMAND ----------

from pyspark.sql.types import IntegerType, StringType, StructType, StructField, DateType, DoubleType, FloatType 

# COMMAND ----------

pit_stops_schema = StructType([
    StructField("raceId", IntegerType(), False),
    StructField("driverId", IntegerType(), True),
    StructField("stop", IntegerType(), True),
    StructField("lap", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("duration", StringType(), True),
    StructField("milliseconds", IntegerType(), True)
])

# COMMAND ----------

pit_stops_df = spark.read.schema(pit_stops_schema).option("multiLine", True).json(f"{bronze}/{v_file_date}/pit_stops.json")

# COMMAND ----------

from pyspark.sql.functions import concat, col, lit, current_timestamp

# COMMAND ----------

pit_stops_add_date_df = add_ingestion_date(pit_stops_df)

# COMMAND ----------

pit_stops_withcolumns_df = pit_stops_add_date_df.withColumnRenamed("raceId", "race_id") \
                                .withColumnRenamed("driverId", "driver_id") \
                                .withColumn("data_source", lit(v_data_source)) \
                                .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# overwrite_partition(pit_stops_withcolumns_df, "f1_silver", "pit_stops", "race_id")

# COMMAND ----------

merge_condition = "tgt.driver_id = src.driver_id AND tgt.race_id = src.race_id"
merge_data(pit_stops_withcolumns_df, "f1_silver", "pit_stops", silver, "race_id", merge_condition)

# COMMAND ----------

dbutils.notebook.exit("success")
