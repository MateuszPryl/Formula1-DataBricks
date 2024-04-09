-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_raw;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### Create circuits table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.circiuts;
CREATE TABLE IF NOT EXISTS f1_raw.circiuts(
  circuitId INT,
  circuitRef STRING,
  name STRING,
  location STRING,
  country STRING,
  lat DOUBLE,
  lng DOUBLE,
  alt INT,
  url STRING
)
USING csv
OPTIONS (path "abfss://raw@formula1dlmateuszp.dfs.core.windows.net/circuits.csv", header true)

-- COMMAND ----------

SELECT * FROM f1_raw.circiuts;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### Create races table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.races;
CREATE TABLE IF NOT EXISTS f1_raw.races(
  raceId INT,
  year INT,
  round INT,
  circuitId INT,
  name STRING,
  date DATE,
  time STRING,
  url STRING
)
USING csv
OPTIONS (path "abfss://raw@formula1dlmateuszp.dfs.core.windows.net/races.csv", header true)

-- COMMAND ----------

SELECT * FROM f1_raw.races;

-- COMMAND ----------

SELECT * FROM f1_raw.races
WHERE year = 2020 and circuitId = 10;