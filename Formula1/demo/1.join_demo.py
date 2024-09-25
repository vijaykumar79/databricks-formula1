# Databricks notebook source
# MAGIC %run "../includes/configurations"

# COMMAND ----------

circuits_df = spark.read.parquet(f"{silver}/circuits") \
    .withColumnRenamed("name", "circuit_name")  

# COMMAND ----------

races_df = spark.read.parquet(f"{silver}/races").filter("race_year = 2019") \
    .withColumnRenamed("name", "race_name")

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

display(races_df)

# COMMAND ----------

circuit_race_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.cirecuit_id, "left") \
    .select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

# COMMAND ----------

display(circuit_race_df)

# COMMAND ----------

circuit_race_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.cirecuit_id, "anti")

# COMMAND ----------

display(circuit_race_df)

# COMMAND ----------


