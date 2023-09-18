# Databricks notebook source
# MAGIC %md
# MAGIC ##All the commands for testing will be done here

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1lake8.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1lake8.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


