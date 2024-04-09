# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest races.csv

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
# MAGIC ### Step 1 - Read CSV using Spark DataFrame reader

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

circuits_schema = StructType(fields=[
    StructField("raceId", IntegerType(), False),
    StructField("year", IntegerType(), True),
    StructField("round", IntegerType(), True),
    StructField("circuitId", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("date", DateType(), True),
    StructField("time", StringType(), True),
    StructField("url", StringType(), True)
])

# COMMAND ----------

races_df = spark.read \
.option("header", True) \
.schema(circuits_schema) \
.csv(f'{raw_folder_path}/{v_file_date}/races.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - Rename the columns

# COMMAND ----------

races_renamed_df = races_df.withColumnRenamed("circuitId", "circuit_id") \
    .withColumnRenamed("raceId", "race_id") \
    .withColumnRenamed("year", "race_year")
    

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Add igestion date to dataframe

# COMMAND ----------

races_ingest_df = add_ingestion_date(races_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4 - Cobine date and time column into race_timestamp

# COMMAND ----------

from pyspark.sql.functions import to_timestamp, concat, lit, col

# COMMAND ----------

races_final_df = races_ingest_df.withColumn('race_timestamp', to_timestamp(concat(col('date'), lit(' '), col("time")), 'yyyy-MM-dd HH:mm:ss')) \
    .withColumn('data_source', lit(v_data_source)) \
    .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

races_final_df = races_final_df.select(col("race_id"), col("race_year"), col("round"), col("circuit_id"), col("name"), col("race_timestamp"), col("ingestion_date"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5 - Write data to Datalake as Parquet

# COMMAND ----------

races_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.races")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.races;

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

