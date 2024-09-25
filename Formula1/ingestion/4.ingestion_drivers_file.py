# Databricks notebook source
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

from pyspark.sql.types import IntegerType, StringType, StructType, StructField, DateType

# COMMAND ----------

name_schema = StructType([
    StructField("forename", StringType(), True),
    StructField("surname", StringType(), True)
])

# COMMAND ----------

drivers_schema = StructType([
    StructField("driverId", IntegerType(), False),
    StructField("code", StringType(), True),
    StructField("dob", DateType(), True),
    StructField("driverRef", StringType(), True),
    StructField("name", name_schema),
    StructField("nationality", StringType(), True),
    StructField("number", IntegerType(), True),
    StructField("url", StringType(), True)
])

# COMMAND ----------

drivers_df = spark.read.schema(drivers_schema).json(f"{bronze}/{v_file_date}/drivers.json")

# COMMAND ----------

from pyspark.sql.functions import concat, col, lit, current_timestamp

# COMMAND ----------

drivers_add_date_df = add_ingestion_date(drivers_df)

# COMMAND ----------

drivers_renamed_df = drivers_add_date_df.withColumnRenamed("driverId", "driver_id") \
                               .withColumnRenamed("driverRef", "driver_ref") \
                               .withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname"))) \
                               .withColumn("data_source", lit(v_data_source)) \
                                .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

drivers_final_df = drivers_renamed_df.drop(col("url"))

# COMMAND ----------

drivers_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_silver.drivers")

# COMMAND ----------

dbutils.notebook.exit("success")
