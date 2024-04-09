-- Databricks notebook source
CREATE OR REPLACE TEMP VIEW v_dominant_teams
AS
SELECT team_name, SUM(calculated_points) AS total_points, COUNT(1) AS total_races, AVG(calculated_points) AS avg_points_per_race,
       RANK() OVER(ORDER BY AVG(calculated_points) DESC) AS team_rank
FROM f1_presentation.calculated_race_results
GROUP BY team_name
HAVING total_races > 100
ORDER BY avg_points_per_race DESC

-- COMMAND ----------

SELECT race_year, team_name, SUM(calculated_points) AS total_points, COUNT(1) AS total_races, AVG(calculated_points) AS avg_points_per_race,
       RANK() OVER(ORDER BY AVG(calculated_points) DESC) AS team_rank
FROM f1_presentation.calculated_race_results
WHERE team_name IN (SELECT team_name FROM v_dominant_teams WHERE team_rank <= 5)
GROUP BY race_year, team_name
ORDER BY race_year, avg_points_per_race DESC

-- COMMAND ----------

SELECT race_year, team_name, SUM(calculated_points) AS total_points, COUNT(1) AS total_races, AVG(calculated_points) AS avg_points_per_race,
       RANK() OVER(ORDER BY AVG(calculated_points) DESC) AS team_rank
FROM f1_presentation.calculated_race_results
WHERE team_name IN (SELECT team_name FROM v_dominant_teams WHERE team_rank <= 5)
GROUP BY race_year, team_name
ORDER BY race_year, avg_points_per_race DESC

-- COMMAND ----------

