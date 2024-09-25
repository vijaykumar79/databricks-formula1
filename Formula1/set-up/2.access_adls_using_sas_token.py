# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using SAS Token
# MAGIC 1. Set the spark config to SAS token
# MAGIC 1. List the files from the demo container
# MAGIC 1. Read the data from circuit.csv file

# COMMAND ----------

dbutils.secrets.list(scope = 'furmula1-scope')

# COMMAND ----------

formula1dl_demo_sas_token = dbutils.secrets.get(scope = 'furmula1-scope', key = 'formula1dl-demo-sas-token')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.vjformula1dl.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.vjformula1dl.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.vjformula1dl.dfs.core.windows.net", formula1dl_demo_sas_token)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@vjformula1dl.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@vjformula1dl.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


