-- Databricks notebook source
DROP TABLE IF EXISTS formula1_dev.bronze.drivers;
CREATE TABLE IF NOT EXISTS formula1_dev.bronze.drivers (
    driverId INT,
    code STRING,
    dob DATE,
    driver_ref STRING,
    name STRUCT<forename: STRING, surname: STRING>,
    nationality STRING,
    number INT,
    url STRING
)
USING json
OPTIONS (path 'abfss://bronze@vjdatabricksucextdl.dfs.core.windows.net/drivers.json')

-- COMMAND ----------

DROP TABLE IF EXISTS formula1_dev.bronze.results;
CREATE TABLE IF NOT EXISTS formula1_dev.bronze.results(
  resultId INT,
  raceId INT,
  statusId INT,
  constructorId INT,
  driverId INT,
  grid INT,
  laps INT,
  positionOrder INT,
  fastestLap INT,
  fastestLapSpeed FLOAT,
  fastestLapTime STRING,
  milliseconds INT,
  number INT,
  position INT,
  positionText STRING,
  rank INT,
  points FLOAT,
  time STRING
)
USING JSON
OPTIONS (path "abfss://bronze@vjdatabricksucextdl.dfs.core.windows.net/results.json")
