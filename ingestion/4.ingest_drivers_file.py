# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest data from drivers.json

# COMMAND ----------

dbutils.widgets.text('p_data_source', '')
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text('p_file_date', '2021-03-21')
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 - Read the JSON file 

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

name_schema = StructType(fields=[
    StructField('forename', StringType(), True),
    StructField('surname', StringType(), True)
])

# COMMAND ----------

drivers_schema = StructType(fields=[
    StructField('driverId', IntegerType(), False),
    StructField('driverRef', StringType(), True),
    StructField('number', IntegerType(), True),
    StructField('code', StringType(), True),
    StructField('name', name_schema, True),
    StructField('dob', DateType(), True),
    StructField('nationality', StringType(), True),
    StructField('url', StringType(), True)
])

# COMMAND ----------

drivers_df = spark.read.json(f'{raw_folder_path}/{v_file_date}/drivers.json', schema=drivers_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - Rename and add columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col, concat, lit

# COMMAND ----------

drivers_df = drivers_df.withColumnRenamed('driverId', 'driver_id') \
    .withColumnRenamed('driverRef', 'driver_ref') \
    .withColumn('ingestion_date', current_timestamp()) \
    .withColumn('name', concat(col('name.forename'), lit(' '), col('name.surname'))) \
    .withColumn('data_source', lit(v_data_source)) \
    .withColumn('file_date', lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Drop unwanted ones

# COMMAND ----------

drivers_df = drivers_df.drop("url")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4 - Write to Parquet file

# COMMAND ----------

drivers_df.write.mode('overwrite').format("delta").saveAsTable("f1_processed.drivers")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.drivers

# COMMAND ----------

dbutils.notebook.exit("Success")