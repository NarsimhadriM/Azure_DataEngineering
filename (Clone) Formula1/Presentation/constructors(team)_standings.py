# Databricks notebook source
# MAGIC %run "..//includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

from pyspark.sql.functions import sum, when, col, count

# COMMAND ----------

cons_standings_df = race_results_df.groupBy("race_year", "team").agg(sum("points").alias("total_points"), count(when(col("position") == 1, True)).alias("wins"))

# COMMAND ----------

display(cons_standings_df.filter("race_year = 2020").orderBy(desc("wins")))

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql.functions import desc, rank

# COMMAND ----------

cons_ranking_df = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_rank_df = cons_standings_df.withColumn("rank", rank().over(cons_ranking_df))

# COMMAND ----------

display(final_rank_df.filter("race_year = 2020"))

# COMMAND ----------

final_rank_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/constructors_standings")

# COMMAND ----------

display(spark.read.parquet(f"{presentation_folder_path}/constructors_standings").filter("race_year = 2020"))

# COMMAND ----------


