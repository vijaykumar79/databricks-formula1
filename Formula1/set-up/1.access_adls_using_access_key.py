# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using Access Key
# MAGIC 1. Set the spark config "fs.azure.account.key"
# MAGIC 1. Display the demo container
# MAGIC 1. Read the circuit.csv file

# COMMAND ----------

dbutils.secrets.list(scope = 'furmula1-scope')

# COMMAND ----------

formula1dl_access_key = dbutils.secrets.get(scope ='furmula1-scope', key='forumal1dl-account-key')

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.vjformula1dl.dfs.core.windows.net",
    formula1dl_access_key
    )

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@vjformula1dl.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@vjformula1dl.dfs.core.windows.net/circuits.csv"))
