# Databricks notebook source
# MAGIC %md
# MAGIC ### Introduction
# MAGIC 1) Set the spark config SAS Token

# COMMAND ----------

formula1lake8_demo_sas_token = dbutils.secrets.get(scope='formula1-scope', key = 'Formula1-demo')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1lake8.dfs.core.windows.net","SAS")
spark.conf.set("fs.azure.sas.token.provider.type.formula1lake8.dfs.core.windows.net","org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.formula1lake8.dfs.core.windows.net","formula1lake8_demo_sas_token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1lake8.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1lake8.dfs.core.windows.net/circuits.csv"
))

# COMMAND ----------


