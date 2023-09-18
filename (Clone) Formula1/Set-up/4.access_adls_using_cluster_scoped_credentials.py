# Databricks notebook source
# MAGIC %md
# MAGIC ### Access Azure Data Lake using Cluster Scoped Credentials
# MAGIC 1) Set the spark config fs.azure.account.key in the cluster
# MAGIC 2) List files from the Demo container
# MAGIC 3) Read data from the circuits.csv file

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1lake8.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1lake8.dfs.core.windows.net/circuits.csv"
))

# COMMAND ----------


