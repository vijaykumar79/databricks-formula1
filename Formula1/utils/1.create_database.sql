-- Databricks notebook source
DROP DATABASE IF EXISTS f1_silver CASCADE;

-- COMMAND ----------

CREATE DATABASE f1_silver
LOCATION "/mnt/vjformula1dl/silver"

-- COMMAND ----------

DROP DATABASE IF EXISTS f1_gold CASCADE

-- COMMAND ----------

CREATE DATABASE f1_gold
LOCATION "/mnt/vjformula1dl/gold"
