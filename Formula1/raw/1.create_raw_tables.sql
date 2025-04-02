-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_raw;

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.drivers;
CREATE TABLE IF NOT EXISTS f1_raw.circuits(
  circuitId INT,
  circuitRef STRING,
  location STRING,
  country STRING,
  lat DOUBLE,
  lng DOUBLE,
  alt INT,
  url STRING
)
USING csv
OPTIONS (path "dbfs:/mnt/formula1dl/raw/circuits.csv")

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.drivers;
CREATE TABLE IF NOT EXISTS fe_raw.drivers (
  driver_id INT,
  driverRef STRING,
  number INT,
  code STRING,
  name STRUCT<forename: STRING, surname: STRING>
)