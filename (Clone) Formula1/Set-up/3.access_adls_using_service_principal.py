# Databricks notebook source
# MAGIC %md
# MAGIC ### Access Azure Data Lake using Service Principal/Azure AD
# MAGIC 1) Register Azure AD/Service Principal
# MAGIC 2) Generate secret password for the application
# MAGIC 3) Set Spark conf with App/client id, tenant/directory id and secret id.
# MAGIC 4) Assign role 'Blob storage Data Distributor' for the data lake.
# MAGIC

# COMMAND ----------

client_id = "7554f07c-9145-4da0-9b95-98a282e088eb"
tenant_id = "b70071ef-503d-4124-879a-2e1195b4bd20"
client_secret = "ewr8Q~_I2ZAkuOXCdBs~6CYiQY2oNoaLANoD_bIN"

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1lake8.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.formula1lake8.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.formula1lake8.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.formula1lake8.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.formula1lake8.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1lake8.dfs.core.windows.net/circuits.csv"
))

# COMMAND ----------


