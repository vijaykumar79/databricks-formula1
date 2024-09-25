# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using Cluster Scope Credential
# MAGIC 1. Set the spark config "fs.azure.account.key" in Cluster Spark Config as Nave Value Pair
# MAGIC 1. Display the demo container
# MAGIC 1. Read the circuit.csv file

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@vjformula1dl.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv(""))
