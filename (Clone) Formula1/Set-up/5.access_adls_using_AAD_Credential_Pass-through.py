# Databricks notebook source
# MAGIC %md
# MAGIC ### Access Data Lake using AAD Credential Pass through (Edit Cluster)
# MAGIC

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.formula1lake8.dfs.core.windows.net", 
    "/e0QNKWqrtznOWA8wiR4vnlb7SN4cmufjKivJAmNv5FEdkSoj5VFvWiMAq2sd9IWcdZl6SWrdGsK+AStOHoZzQ=="
)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1lake8.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1lake8.dfs.core.windows.net/circuits.csv"
))

# COMMAND ----------


