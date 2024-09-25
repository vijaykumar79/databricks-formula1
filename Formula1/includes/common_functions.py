# Databricks notebook source
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

def add_ingestion_date(input_df):
    output_df = input_df.withColumn("ingestion_date", current_timestamp())
    return output_df

# COMMAND ----------

def re_arrange_partition_column(input_df, partition_column):
    column_list = [];
    for column_name in input_df.schema.names:
        if column_name != partition_column:
            column_list.append(column_name)
    column_list.append(partition_column)
    output_df = input_df.select(column_list)
    return output_df

# COMMAND ----------

def overwrite_partition(input_df, db_name, table_name, partition_col):
    output_df = re_arrange_partition_column(input_df, partition_col)
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    if spark.catalog.tableExists(f"{db_name}.{table_name}"):
        output_df.write.mode("overwrite").insertInto(f"{db_name}.{table_name}")
    else:
        output_df.write.mode("overwrite").partitionBy(f"{partition_col}").format("parquet").saveAsTable(f"{db_name}.{table_name}")

# COMMAND ----------

def merge_data(input_df, db_name, table_name, table_path, partition_col, merge_condition):
    from delta.tables import DeltaTable

    spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning", "true")

    if spark.catalog.tableExists(f"{db_name}.{table_name}"):
        delta_table = DeltaTable.forPath(spark, f"{table_path}/{table_name}")
        delta_table.alias("tgt") \
        .merge(
            input_df.alias("src"),
            merge_condition
        )\
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()
    else:
        input_df.write.mode("overwrite").partitionBy(partition_col).format("delta").saveAsTable(f"{db_name}.{table_name}")
