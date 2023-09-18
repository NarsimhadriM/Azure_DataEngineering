# Databricks notebook source
# MAGIC %run "..//includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Local Temp view

# COMMAND ----------

race_results_df.createOrReplaceTempView("v_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(1) from v_race_results
# MAGIC where race_year = 2020

# COMMAND ----------

race_results_2019 = spark.sql("select * from v_race_results where race_year = 2019")

# COMMAND ----------

race_results_df.createOrReplaceGlobalTempView("gv_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in global_temp;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from global_temp.gv_race_results
# MAGIC where race_year = 2020;

# COMMAND ----------

global_temp_view = spark.sql("select * from global_temp.gv_race_results where race_year = 2020").show()

# COMMAND ----------


