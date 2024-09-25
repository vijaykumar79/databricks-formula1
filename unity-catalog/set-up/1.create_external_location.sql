-- Databricks notebook source
CREATE EXTERNAL LOCATION IF NOT EXISTS formula1_bronze
URL "abfss://bronze@vjdatabricksucextdl.dfs.core.windows.net/"
WITH (STORAGE CREDENTIAL `databricks-uc-ext-storage-credential`)

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS formula1_silver
URL "abfss://silver@vjdatabricksucextdl.dfs.core.windows.net/"
WITH (STORAGE CREDENTIAL `databricks-uc-ext-storage-credential`)

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS formula1_gold
URL "abfss://gold@vjdatabricksucextdl.dfs.core.windows.net/"
WITH (STORAGE CREDENTIAL `databricks-uc-ext-storage-credential`)

-- COMMAND ----------

DESC EXTERNAL LOCATION formula1_bronze
