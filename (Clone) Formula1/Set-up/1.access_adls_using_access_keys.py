# Databricks notebook source
# MAGIC %md
# MAGIC ### Introduction
# MAGIC 1) Set the spark config fs.azure.account.key

# COMMAND ----------

formula1lake8_account_key = dbutils.secrets.get(scope = 'formula1-scope', key='formula1lake8-account-access-key')

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.formula1lake8.dfs.core.windows.net", formula1lake8_account_key)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1lake8.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1lake8.dfs.core.windows.net/circuits.csv"
))

# COMMAND ----------


