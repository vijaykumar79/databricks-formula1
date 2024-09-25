# Databricks notebook source
dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list(scope = 'furmula1-scope')

# COMMAND ----------

dbutils.secrets.get(scope = 'furmula1-scope', key = 'formula1-app-client-id')
