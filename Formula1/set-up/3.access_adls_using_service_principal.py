# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using Service Principal
# MAGIC 1. Register AD App/ Service Principal
# MAGIC 1. Generate Secret/Password for the application
# MAGIC 1. Set spark config with App/Client Id, Directory/Tenant Id & Secret
# MAGIC 1. Assing Role "Storage Blob Contributer" to App on Data Lake

# COMMAND ----------

client_id =  dbutils.secrets.get(scope = 'furmula1-scope', key = 'formula1-app-client-id')
tenant_id = dbutils.secrets.get(scope = 'furmula1-scope', key = 'formula1-app-tenant-id')
client_secret = dbutils.secrets.get(scope = 'furmula1-scope', key = 'formula1-app-client-secret')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.vjformula1dl.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.vjformula1dl.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.vjformula1dl.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.vjformula1dl.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.vjformula1dl.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@vjformula1dl.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@vjformula1dl.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


