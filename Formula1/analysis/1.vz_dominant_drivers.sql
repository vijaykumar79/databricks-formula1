-- Databricks notebook source
-- MAGIC %python
-- MAGIC html = "<h1>Dominant Drivers Dashboard</h1>"
-- MAGIC displayHTML(html)

-- COMMAND ----------

SELECT * FROM f1_gold.calculated_race_results LIMIT 5

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW tv_dominant_driver
AS
SELECT driver_name
      ,count(1) AS total_races
      ,avg(calculated_points) AS avg_points
FROM f1_gold.calculated_race_results
GROUP BY driver_name
HAVING count(1) >= 50
ORDER BY avg_points DESC
LIMIT 10

-- COMMAND ----------

SELECT * FROM tv_dominant_driver

-- COMMAND ----------

SELECT driver_name
      ,race_year
      ,count(1) AS total_races
      ,avg(calculated_points) AS avg_points
FROM f1_gold.calculated_race_results
WHERE driver_name IN (SELECT driver_name FROM tv_dominant_driver)
      AND race_year BETWEEN 2001 AND 2024
GROUP BY driver_name,race_year
ORDER BY race_year, avg_points DESC

