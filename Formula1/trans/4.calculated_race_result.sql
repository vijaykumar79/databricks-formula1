-- Databricks notebook source
-- MAGIC %python
-- MAGIC dbutils.widgets.text("p_file_date", "2021-03-21")
-- MAGIC v_file_date = dbutils.widgets.get("p_file_date")

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_gold.calculated_race_results (
      race_year INT,
      driver_name STRING,
      constructor_name STRING,
      position INT,
      points INT,
      calculated_points INT
)
USING DELTA

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql(f"""
-- MAGIC             SELECT  rc.race_year
-- MAGIC                 ,dv.name AS driver_name
-- MAGIC                 ,cn.name AS constructor_name
-- MAGIC                 ,rs.position
-- MAGIC                 ,rs.points
-- MAGIC                 ,11 - rs.position AS calculated_points
-- MAGIC             FROM f1_silver.results rs
-- MAGIC             INNER JOIN f1_silver.races rc ON rc.race_id = rs.race_id
-- MAGIC             INNER JOIN f1_silver.drivers dv ON dv.driver_id = rs.driver_id
-- MAGIC             INNER JOIN f1_silver.constructors cn ON cn.constructor_id = rs.constructor_id
-- MAGIC             WHERE rs.position <= 10 AND race_year = {v_file_date}
-- MAGIC """).dropDuplicates(['race_year', 'driver_name', 'constructor_name']).createOrReplaceTempView("tv_calculated_race_results")

-- COMMAND ----------


MERGE INTO f1_gold.calculated_race_results t
USING tv_calculated_race_results s
ON t.race_year = s.race_year AND t.driver_name = s.driver_name and t.constructor_name = s.constructor_name
WHEN MATCHED 
THEN
  UPDATE SET t.position = s.position, 
             t.points = s.points,
             t.calculated_points = s.calculated_points
WHEN NOT MATCHED 
THEN
  INSERT (race_year, driver_name, constructor_name, position, points, calculated_points)
  VALUES (s.race_year, s.driver_name, s.constructor_name, s.position, s.points, s.calculated_points);


