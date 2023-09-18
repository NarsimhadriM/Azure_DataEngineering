# Databricks notebook source
# MAGIC %run "..//includes/configuration"

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races")

# COMMAND ----------

race_filtered_df = races_df.filter("race_year = 2019")

# COMMAND ----------

race_filtered_df_a = races_df.filter((races_df["race_year"] == 2019) & (races_df["round"] <= 5))

# COMMAND ----------

display(race_filtered_df_a)

# COMMAND ----------


