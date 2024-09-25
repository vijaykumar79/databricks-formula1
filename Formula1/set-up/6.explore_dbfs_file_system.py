# Databricks notebook source
# MAGIC %md
# MAGIC #### Explore DBFS root
# MAGIC

# COMMAND ----------

display(dbutils.fs.ls('/'))

# COMMAND ----------

display(dbutils.fs.ls('dbfs:/FileStore/'))

# COMMAND ----------

display(spark.read.csv('/FileStore/circuits.csv'))
