-- Databricks notebook source
-- MAGIC %run "..//includes/configuration"

-- COMMAND ----------

drop table if exists f1_raw.results;
create table if not exists f1_raw.results(
    resultId int,
    raceId int,
    driverId int,
    constructorId int,
    number int,
    grid int,
    position int,
    positionText int,
    positionOrder int,
    points int,
    laps int,
    time string,
    milliseconds string,
    fastlap int,
    rank int,
    fastestLapTime string,
    fastestLapSpeed float,
    statusId int)
    using json
    options (path "/mnt/formula1lake8/raw/results.json")
)
