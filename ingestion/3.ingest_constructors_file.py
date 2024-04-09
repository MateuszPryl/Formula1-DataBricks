# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest data from constructors.json

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

constructors_schema = StructType(fields=[
    StructField('constructorId', IntegerType(), False),
    StructField('constructorRef', StringType(), True),
    StructField('name', StringType(), True),
    StructField('nationality', StringType(), True),
    StructField('url', StringType(), True)
])

# COMMAND ----------

constructor_df = spark.read.json(f'{raw_folder_path}/{v_file_date}/constructors.json', schema=constructors_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - Drop unwanted columns

# COMMAND ----------

constructor_df = constructor_df.drop("url")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Rename columns and add ingested_date and data source

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

constructor_final_df = constructor_df.withColumnRenamed('constructorId', 'constructor_id') \
    .withColumnRenamed('constructorRef', 'constructor_ref') \
    .withColumn('data_source', lit(v_data_source)) \
    .withColumn('file_date', lit(v_file_date))

constructor_final_df = add_ingestion_date(constructor_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4 - Write to Parquet file

# COMMAND ----------

constructor_final_df.write.mode('overwrite').format("delta").saveAsTable("f1_processed.constructors")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.constructors

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

