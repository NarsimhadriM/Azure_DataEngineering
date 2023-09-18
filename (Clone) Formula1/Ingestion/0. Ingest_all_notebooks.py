# Databricks notebook source
v_result = dbutils.notebook.run("1. Ingest_circuits_file", 0, {"data_source": "Ergast API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("2.Ingestion of Race", 0, {"data_source": "Ergast API"})

# COMMAND ----------

v_result

# COMMAND ----------


