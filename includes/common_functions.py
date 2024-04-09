# Databricks notebook source
from pyspark.sql.functions import current_timestamp

def add_ingestion_date(input_df):
    output_df = input_df.withColumn('ingestion_date', current_timestamp())
    return output_df

# COMMAND ----------

def move_partition_column_to_end(df, partition_by_column):
    cols = df.schema.names
    cols.remove(partition_by_column)
    cols.append(partition_by_column)
    return df.select(cols)

# COMMAND ----------

def perform_incremental_load(df, db_name, table_name, partition_by_column):
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    result_df = move_partition_column_to_end(df, partition_by_column)
    if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
        result_df.write.mode('overwrite').insertInto(f"{db_name}.{table_name}")
    else:
        result_df.write.mode('overwrite').partitionBy(partition_by_column).format("parquet").saveAsTable(f"{db_name}.{table_name}")

# COMMAND ----------

from delta import DeltaTable

def merge_delta_data(input_df, db_name, table_name, folder_path, merge_condition, partition_column):   
    spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning", True)

    if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
        deltaTable = DeltaTable.forPath(spark, f"{folder_path}/{table_name}")
        deltaTable.alias("tgt").merge(
            input_df.alias("src"),
            merge_condition) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()
    else:
        input_df.write.mode('overwrite').partitionBy(partition_column).format("delta").saveAsTable(f"{db_name}.{table_name}")

# COMMAND ----------

