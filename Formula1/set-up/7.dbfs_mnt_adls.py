# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using DBFS Mounts
# MAGIC 1. Create mount point using service principal
# MAGIC 1. Register AD App/ Service Principal
# MAGIC 1. Generate Secret/Password for the application
# MAGIC 1. Set spark config with App/Client Id, Directory/Tenant Id & Secret
# MAGIC 1. Assing Role "Storage Blob Contributer" to App on Data Lake

# COMMAND ----------

def mount_adls(storage_account_name: str, container_name: str):
    # Get keys/secret from Key Vault
    client_id =  dbutils.secrets.get(scope = 'furmula1-scope', key = 'formula1-app-client-id')
    tenant_id = dbutils.secrets.get(scope = 'furmula1-scope', key = 'formula1-app-tenant-id')
    client_secret = dbutils.secrets.get(scope = 'furmula1-scope', key = 'formula1-app-client-secret')

    # Set Spark Configuration
    configs = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": client_id,
    "fs.azure.account.oauth2.client.secret": client_secret,
    "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
    }

    #Unmount the path if exists
    for mount in dbutils.fs.mounts():
        if mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}":
            dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")

    # Mount adls container
    dbutils.fs.mount(
    source=f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
    mount_point=f"/mnt/{storage_account_name}/{container_name}",
    extra_configs=configs
    )

    display(dbutils.fs.mounts())

# COMMAND ----------

mount_adls('vjformula1dl','demo')

# COMMAND ----------

mount_adls('vjformula1dl','bronze')

# COMMAND ----------

mount_adls('vjformula1dl','silver')

# COMMAND ----------

mount_adls('vjformula1dl','gold')

# COMMAND ----------


