# Databricks notebook source
# MAGIC %md
# MAGIC ### Access Azure Data Lake using Service Principal/Azure AD
# MAGIC 1) Register Azure AD/Service Principal
# MAGIC 2) Generate secret password for the application
# MAGIC 3) Set Spark conf with App/client id, tenant/directory id and secret id.
# MAGIC 4) Assign role 'Blob storage Data Distributor' for the data lake.
# MAGIC

# COMMAND ----------

client_id = dbutils.secrets.get(scope = 'formula1-scope', key='formula1-App-Client-id')
tenant_id = dbutils.secrets.get(scope='formula1-scope', key='formula1-App-tenant-id')
client_secret = dbutils.secrets.get(scope='formula1-scope', key='formula1-App-client-secret')

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}


# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://demo@formula1lake8.dfs.core.windows.net/",
  mount_point = "/mnt/formula1lake8/demo",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.ls('/mnt/formula1lake8/demo'))

# COMMAND ----------

display(spark.read.csv("/mnt/formula1lake8/demo/circuits.csv"
))

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------


