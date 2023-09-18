# Databricks notebook source
# MAGIC %run "..//includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

from pyspark.sql.functions import sum, when, col, count

# COMMAND ----------

race_standings_df = race_results_df.groupBy("race_year", "driver_name", "driver_nationality", "team").agg(sum("points").alias("total_points"), count(when(col("position") == 1, True)).alias("wins"))

# COMMAND ----------

display(race_standings_df.filter("race_year = 2020").orderBy(desc("wins")))

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql.functions import rank, desc

# COMMAND ----------

driver_rank_df = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("Wins"))

# COMMAND ----------

final_df = race_standings_df.withColumn("rank", rank().over(driver_rank_df))

# COMMAND ----------

display(final_df.filter("race_year =2020"))

# COMMAND ----------

final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/driver_standings")

# COMMAND ----------

display(spark.read.parquet(f"{presentation_folder_path}/driver_standings").filter("race_year = 2020"))

# COMMAND ----------


