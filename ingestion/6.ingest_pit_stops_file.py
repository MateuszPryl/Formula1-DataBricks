# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest data from pitstops.json

# COMMAND ----------

dbutils.widgets.text('p_data_source', '')
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text('p_file_date', '2021-03-28')
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 - Read the JSON file 

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

pit_stops_schema = StructType(fields=[
    StructField('raceId', IntegerType(), False),
    StructField('driverId', IntegerType(), True),
    StructField('stop', StringType(), True),
    StructField('lap', IntegerType(), True),
    StructField('time', StringType(), True),
    StructField('duration', StringType(), True),
    StructField('milliseconds', IntegerType(), True)
])

# COMMAND ----------

pit_stops_df = spark.read.json(f'{raw_folder_path}/{v_file_date}/pit_stops.json', schema=pit_stops_schema, multiLine=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - Rename and add columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col, lit

# COMMAND ----------

pit_stops_df = pit_stops_df.withColumnRenamed('driverId', 'driver_id') \
    .withColumnRenamed('raceId', 'race_id') \
    .withColumn('ingestion_date', current_timestamp()) \
    .withColumn('data_source', lit(v_data_source)) \
    .withColumn('file_date', lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Write to Parquet file

# COMMAND ----------

# perform_incremental_load(pit_stops_df, "f1_processed", "pit_stops", "race_id")
merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.stop = src.stop"
merge_delta_data(pit_stops_df, "f1_processed", "pit_stops", processed_folder_path, merge_condition, "race_id")

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.pit_stops

# COMMAND ----------

