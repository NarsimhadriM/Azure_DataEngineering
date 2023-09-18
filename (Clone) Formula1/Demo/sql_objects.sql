-- Databricks notebook source
-- MAGIC %run "..//includes/configuration"

-- COMMAND ----------

create database if not exists demo;

-- COMMAND ----------

describe database demo;

-- COMMAND ----------

select current_database();

-- COMMAND ----------

show tables;

-- COMMAND ----------

use database demo;

-- COMMAND ----------

select current_database();

-- COMMAND ----------

show tables;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.mode("overwrite").format("parquet").saveAsTable("demo.race_results_py")

-- COMMAND ----------

show tables in demo;

-- COMMAND ----------

  desc extended race_results_py;

-- COMMAND ----------

select * from demo.race_results_py
where race_year = 2020 and position = 1;

-- COMMAND ----------

create table if not exists race_results_sql
as select * from demo.race_results_py where race_year = 2020;

-- COMMAND ----------

select * from race_results_sql;

-- COMMAND ----------

show tables in demo;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.mode("overwrite").format("parquet").option("path", f"{presentation_folder_path}/race_results_ext_py").saveAsTable("demo.race_results_ext_py")

-- COMMAND ----------

desc extended race_results_ext_py

-- COMMAND ----------

show tables in demo;


-- COMMAND ----------

drop table demo.race_results_ext_py;

-- COMMAND ----------

show tables in demo;

-- COMMAND ----------

select * from race_results_py limit 100;

-- COMMAND ----------

create or Replace temp view v_race_results
as select * from race_results_py where race_year = 2020;

-- COMMAND ----------

select * from v_race_results;

-- COMMAND ----------

create or replace global temp view gv_race_results
as select * from race_results_py where race_year = 2018;

-- COMMAND ----------

select * from global_temp.gv_race_results;

-- COMMAND ----------

show tables;

-- COMMAND ----------

show tables in global_temp;

-- COMMAND ----------

create or replace view pev_race_results
as select * from race_results_py where race_year = 2019;

-- COMMAND ----------

show tables;

-- COMMAND ----------

use default;

-- COMMAND ----------

show tables in demo;

-- COMMAND ----------

show tables;

-- COMMAND ----------

create table calculated_race_results
as
select races.race_year, constructors.name as team_name, drivers.name as driver_name, results.position, results.points
from race_results_py
join on drivers (results.driver_id = drivers.driver_id),
join on constructors (constructors.constructor_id = results.constructor_id),
join on races(races.race_id = results.race_id)
where results.position <= 10;

-- COMMAND ----------


