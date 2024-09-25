# Databricks notebook source
v_result = dbutils.notebook.run("1.ingest_circuits_data", 0, {"p_data_source" : "Eagast API", "p_file_date" : "2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("2.ingest_race_data",0, {"p_data_source" : "Eagast API", "p_file_date" : "2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("3.ingestion_constructor_file",0, {"p_data_source" : "Eagast API", "p_file_date" : "2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("4.ingestion_drivers_file",0, {"p_data_source" : "Eagast API", "p_file_date" : "2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("5.ingestion_results_file",0, {"p_data_source" : "Eagast API", "p_file_date" : "2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("6.ingestion_pit_stops_file",0, {"p_data_source" : "Eagast API", "p_file_date" : "2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("7.ingestion_lap_times_file",0, {"p_data_source" : "Eagast API", "p_file_date" : "2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("8.ingestion_qualifying_file",0, {"p_data_source" : "Eagast API", "p_file_date" : "2021-04-18"})

# COMMAND ----------

v_result
