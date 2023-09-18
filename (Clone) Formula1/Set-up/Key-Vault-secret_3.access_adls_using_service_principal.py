# Databricks notebook source
# MAGIC %md
# MAGIC ### Access Azure Data Lake using Service Principal/Azure AD
# MAGIC 1) Register Azure AD/Service Principal
# MAGIC 2) Generate secret password for the application
# MAGIC 3) Set Spark conf with App/client id, tenant/directory id and secret id.
# MAGIC 4) Assign role 'Blob storage Data Distributor' for the data lake.
# MAGIC

# COMMAND ----------

client_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-App-Client-id')
tenant_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-App-tenant-id')
client_secret = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-App-client-secret')

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

display(dbutils.fs.ls("abfss://demo@formula1lake8.dfs.core.windows.net"))

# COMMAND ----------


