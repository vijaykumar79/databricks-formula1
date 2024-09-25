# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest Circuits CSV file int Bronze Layer

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configurations"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import IntegerType, StringType, DateType, StructType, StructField, TimestampType

# COMMAND ----------

races_schema = StructType([
    StructField("raceId", IntegerType(), False),
    StructField("year", IntegerType(), True),
    StructField("round", IntegerType(), True),
    StructField("circuitId", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("date", DateType(), True),
    StructField("time", StringType(), True),
    StructField("url", StringType(), True),
])

# COMMAND ----------

races_df = spark.read \
    .option("header", True) \
    .schema(races_schema) \
    .csv(f"{bronze}/{v_file_date}/races.csv")

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, to_timestamp, concat, lit

# COMMAND ----------

racess_timestamp_df = races_df \
    .withColumn("race_timestamp", to_timestamp(concat(col("date"), lit(" "),col("time")), "yyyy-MM-dd HH:mm:ss"))\
    .withColumn("data_source", lit(v_data_source)) \
    .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

races_selected_df = racess_timestamp_df.select(col('raceId').alias('race_id'), col('year').alias('race_year'), col('round'), col('circuitId').alias('cirecuit_id'), col('name'), col('race_timestamp'), col("data_source"), col("file_date"))

# COMMAND ----------

races_final_df = add_ingestion_date(races_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write data to parquet in Silver layer

# COMMAND ----------

races_final_df.write.mode("overwrite").partitionBy("race_year").format("delta").saveAsTable("f1_silver.races")

# COMMAND ----------

display(spark.read.format("delta").load(f"{silver}/races"))

# COMMAND ----------

dbutils.notebook.exit("success")
