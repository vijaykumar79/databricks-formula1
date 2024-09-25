-- Databricks notebook source
DROP TABLE IF EXISTS formula1_dev.silver.drivers;
CREATE TABLE IF NOT EXISTS formula1_dev.silver.drivers 
AS
SELECT driverId AS driver_id
      ,code
      ,dob
      ,driver_ref
      ,concat(name.forename, ' ', name.surname) AS driver_name
      ,nationality
      ,number
      ,current_timestamp() AS ingestion_date
FROM formula1_dev.bronze.drivers

-- COMMAND ----------

DROP TABLE IF EXISTS formula1_dev.silver.results;
CREATE TABLE IF NOT EXISTS formula1_dev.silver.results
AS
SELECT resultId AS result_id
      ,raceId AS race_id
      ,statusId AS status_id
      ,constructorId AS constructor_id
      ,driverId AS driver_id
      ,grid
      ,laps
      ,positionOrder AS position_order
      ,fastestLapSpeed AS fastest_lap_speed
      ,fastestLapTime AS fastest_lap_time
      ,milliseconds
      ,number
      ,position
      ,positionText AS position_text
      ,rank
      ,points
      ,time
      ,current_timestamp() AS ingestion_date
FROM formula1_dev.bronze.results
