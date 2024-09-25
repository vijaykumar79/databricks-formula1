-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_bronze;

-- COMMAND ----------

DROP TABLE IF EXISTS f1_bronze.circuits;
CREATE TABLE IF NOT EXISTS f1_bronze.circuits (
  circuitId INT
  ,circuitRef STRING
  ,name STRING
  ,location STRING
  ,country STRING
  ,lat DOUBLE
  ,lng DOUBLE
  ,alt INT
  ,url STRING
)
USING CSV
OPTIONS (path "/mnt/vjformula1dl/bronze/circuits.csv", header "true")

-- COMMAND ----------

SELECT * FROM f1_bronze.circuits

-- COMMAND ----------

DROP TABLE IF EXISTS f1_bronze.races;
CREATE TABLE IF NOT EXISTS f1_bronze.races
(
  raceId INT,
  year INT,
  round INT,
  circuitId INT,
  name STRING,
  date DATE,
  time STRING,
  url STRING
)
USING CSV
OPTIONS (path "/mnt/vjformula1dl/bronze/races.csv", header "true")

-- COMMAND ----------

SELECT * FROM f1_bronze.races

-- COMMAND ----------

DROP TABLE IF EXISTS f1_bronze.constructors;
CREATE TABLE IF NOT EXISTS f1_bronze.constructors(
  constructorId INT,
  constructorRef STRING,
  name STRING,
  nationality STRING,
  url STRING
)
USING JSON
OPTIONS (path "/mnt/vjformula1dl/bronze/constructors.json")

-- COMMAND ----------

SELECT * FROM f1_bronze.constructors

-- COMMAND ----------

DROP TABLE IF EXISTS f1_bronze.drivers;
CREATE TABLE IF NOT EXISTS f1_bronze.drivers(
  driverId INT,
  code STRING,
  dob DATE,
  driverRef STRING,
  name STRUCT<forename : STRING, surname : STRING>,
  nationality STRING,
  number INT,
  url STRING
)
USING JSON
OPTIONS (path "/mnt/vjformula1dl/bronze/drivers.json")

-- COMMAND ----------

SELECT * FROM f1_bronze.drivers

-- COMMAND ----------

DROP TABLE IF EXISTS f1_bronze.results;
CREATE TABLE IF NOT EXISTS f1_bronze.results(
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
OPTIONS (path "/mnt/vjformula1dl/bronze/results.json")

-- COMMAND ----------

SELECT * FROM f1_bronze.results

-- COMMAND ----------

DROP TABLE IF EXISTS f1_bronze.pit_stops;
CREATE TABLE IF NOT EXISTS f1_bronze.pit_stops(
  raceId INT,
  driverId INT,
  stop INT,
  lap INT,
  time STRING,
  duration STRING,
  milliseconds INT
)
USING JSON
OPTIONS (path "/mnt/vjformula1dl/bronze/pit_stops.json", multiLine "true")

-- COMMAND ----------

SELECT * FROM f1_bronze.pit_stops

-- COMMAND ----------

DROP TABLE IF EXISTS f1_bronze.lap_times;
CREATE TABLE IF NOT EXISTS f1_bronze.lap_times(
  raceId INT,
  driverId INT,
  lap INT,
  postion INT,
  time INT,
  milliseconds INT
)
USING CSV
OPTIONS (path "/mnt/vjformula1dl/bronze/lap_times", header "true")

-- COMMAND ----------

SELECT * FROM f1_bronze.lap_times

-- COMMAND ----------

DROP TABLE IF EXISTS f1_bronze.qualifying;
CREATE TABLE IF NOT EXISTS f1_bronze.qualifying(
  qualifyId INT,
  raceId INT,
  driverId INT,
  constructorId INT,
  number INT,
  position INT,
  q1 STRING,
  q2 STRING,
  q3 STRING
)
USING JSON
OPTIONS (path "/mnt/vjformula1dl/bronze/qualifying", multiLine "true")

-- COMMAND ----------

SELECT * FROM f1_bronze.qualifying

-- COMMAND ----------


