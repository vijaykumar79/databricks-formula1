-- Databricks notebook source
CREATE CATALOG IF NOT EXISTS formula1_dev;

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS formula1_dev.bronze
MANAGED LOCATION "abfss://bronze@vjdatabricksucextdl.dfs.core.windows.net/"

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS formula1_dev.silver
MANAGED LOCATION "abfss://silver@vjdatabricksucextdl.dfs.core.windows.net/"

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS formula1_dev.gold
MANAGED LOCATION "abfss://gold@vjdatabricksucextdl.dfs.core.windows.net/"

-- COMMAND ----------

USE  CATALOG formula1_dev;
SHOW SCHEMAS
