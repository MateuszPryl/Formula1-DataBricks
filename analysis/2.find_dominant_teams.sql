-- Databricks notebook source
SELECT team_name, SUM(calculated_points) AS total_points, COUNT(1) AS total_races, AVG(calculated_points) AS avg_points_per_race
FROM f1_presentation.calculated_race_results
GROUP BY team_name
HAVING total_races > 100
ORDER BY avg_points_per_race DESC


-- COMMAND ----------

SELECT team_name, SUM(calculated_points) AS total_points, COUNT(1) AS total_races, AVG(calculated_points) AS avg_points_per_race
FROM f1_presentation.calculated_race_results
WHERE race_year BETWEEN 2000 AND 2010
GROUP BY team_name
HAVING total_races > 100
ORDER BY avg_points_per_race DESC


-- COMMAND ----------

SELECT team_name, SUM(calculated_points) AS total_points, COUNT(1) AS total_races, AVG(calculated_points) AS avg_points_per_race
FROM f1_presentation.calculated_race_results
WHERE race_year BETWEEN 2010 AND 2020
GROUP BY team_name
HAVING total_races > 100
ORDER BY avg_points_per_race DESC

-- COMMAND ----------

