-- Databricks notebook source
DROP TABLE IF EXISTS formula1_dev.gold.driver_wins;
CREATE TABLE IF NOT EXISTS formula1_dev.gold.driver_wins
AS
SELECT d.driver_name
      ,COUNT(1) AS no_of_wins
FROM formula1_dev.silver.drivers d
INNER JOIN formula1_dev.silver.results r
      ON d.driver_id = r.driver_id
WHERE r.position = 1
GROUP BY d.driver_name
