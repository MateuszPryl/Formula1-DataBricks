# Databricks notebook source
# MAGIC %md
# MAGIC ## Get data from 4 tables to present. Join from Races, Circiuts, Drivers, Constructors, Results

# COMMAND ----------

dbutils.widgets.text('p_file_date', '2021-03-21')
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load data

# COMMAND ----------

drivers_df = spark.read.format("delta").load(f'{processed_folder_path}/drivers')
circuits_df = spark.read.format("delta").load(f'{processed_folder_path}/circuits')
constructors_df = spark.read.format("delta").load(f'{processed_folder_path}/constructors')
races_df = spark.read.format("delta").load(f'{processed_folder_path}/races')
results_df = spark.read.format("delta").load(f'{processed_folder_path}/results') \
                .filter(f"file_date = '{v_file_date}'")


# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Rename columns

# COMMAND ----------

races_df = races_df.withColumnRenamed('name', 'race_name') \
    .withColumnRenamed('race_timestamp', 'race_date')

# COMMAND ----------

drivers_df = drivers_df.withColumnRenamed('name', 'driver_name') \
    .withColumnRenamed('number', 'driver_number') \
    .withColumnRenamed('nationality', 'driver_nationality')

# COMMAND ----------

constructors_df = constructors_df.withColumnRenamed('name', 'team_name')

# COMMAND ----------

results_df = results_df.withColumnRenamed('time', 'race_time') \
                       .withColumnRenamed('race_id', 'result_race_id') \
                       .withColumnRenamed('file_date', 'result_file_date')

# COMMAND ----------

circuits_df = circuits_df.withColumnRenamed('location', 'circuit_location')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Perform joins

# COMMAND ----------

# MAGIC %md 
# MAGIC #### 3.1 Approach 1
# MAGIC

# COMMAND ----------

# needed_data_df = drivers_df.join(results_df, on=(results_df.driver_id == drivers_df.driver_id)) \
#     .join(constructors_df, on=(constructors_df.constructor_id == results_df.constructor_id)) \
#     .join(races_df, on=races_df.race_id == results_df.race_id) \
#     .join(circuits_df, on=circuits_df.circuit_id == races_df.circuit_id) \
#     .select("race_year", "race_name", 'race_date', 'circuit_location', 'driver_name', 'driver_number', 'driver_nationality', 'team_name', 'grid', 'fastest_lap', 'race_time', 'points')

# COMMAND ----------

# needed_data_df.count()

# COMMAND ----------

# MAGIC %md 
# MAGIC #### 3.2 Approach 2
# MAGIC

# COMMAND ----------

race_ciruits_df = races_df.join(circuits_df, on=races_df.circuit_id == circuits_df.circuit_id) \
    .select(races_df.race_id, races_df.race_year, races_df.race_name, races_df.race_date, circuits_df.circuit_location)

# COMMAND ----------

race_results_df = results_df.join(race_ciruits_df, on=results_df.result_race_id  == race_ciruits_df.race_id) \
                            .join(drivers_df, on=results_df.driver_id == drivers_df.driver_id) \
                            .join(constructors_df, on=results_df.constructor_id == constructors_df.constructor_id)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_results_df = race_results_df.select('race_id', 'race_year', 'race_name', 'race_date', 'circuit_location', 'driver_name', 'driver_number', 'driver_nationality',      'team_name', 'grid', 'fastest_lap', 'race_time', 'points', 'position', 'result_file_date') \
                                    .withColumn('created_date', current_timestamp()) \
                                    .withColumnRenamed('result_file_date', 'file_date')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Save to presentation container

# COMMAND ----------

# perform_incremental_load(final_results_df, "f1_presentation", "race_results", "race_id")

merge_condition = "tgt.race_id = src.race_id AND tgt.driver_name = src.driver_name"
merge_delta_data(final_results_df, "f1_presentation", "race_results", presentation_folder_path, merge_condition, "race_id")

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_presentation.race_results;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, COUNT(1)
# MAGIC FROM f1_presentation.race_results
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC

# COMMAND ----------

