-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION "abfss://processed@formula1dlmateuszp.dfs.core.windows.net/f1_processed"

-- COMMAND ----------

DESC DATABASE f1_processed;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_presentation
LOCATION "abfss://presentation@formula1dlmateuszp.dfs.core.windows.net/f1_presentation"

-- COMMAND ----------

DESC DATABASE f1_presentation


-- COMMAND ----------

