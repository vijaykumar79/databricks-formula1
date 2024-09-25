-- Databricks notebook source
SELECT * FROM f1_gold.calculated_race_results LIMIT 5

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW tv_dominant_teams
AS
SELECT constructor_name AS team
      ,count(1) AS total_races
      ,avg(calculated_points) AS avg_points
FROM f1_gold.calculated_race_results
GROUP BY constructor_name
HAVING count(1) >= 100
ORDER BY avg_points DESC
LIMIT 10

-- COMMAND ----------

SELECT * FROM tv_dominant_teams

-- COMMAND ----------

SELECT constructor_name
      ,race_year
      ,count(1) AS total_races
      ,avg(calculated_points) AS avg_points
FROM f1_gold.calculated_race_results
WHERE constructor_name IN (SELECT team FROM tv_dominant_teams)
      AND race_year BETWEEN 2001 AND 2024
GROUP BY constructor_name,race_year
ORDER BY race_year, avg_points DESC

