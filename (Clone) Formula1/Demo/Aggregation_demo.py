# Databricks notebook source
# MAGIC %run "..//includes/configuration/"

# COMMAND ----------

drivers_df = spark.read.parquet(f"{processed_folder_path}/drivers").withColumnRenamed("name", "driver_name").withColumnRenamed("number", "driver_number").withColumnRenamed("nationality", "driver_nationality")

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

constructors_df = spark.read.parquet(f"{processed_folder_path}/constructors").withColumnRenamed("name", "team")

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races").withColumnRenamed("name", "race_name").withColumnRenamed("race_timestamp", "race_date")

# COMMAND ----------

results_df = spark.read.parquet(f"{processed_folder_path}/results").withColumnRenamed("fastest_lap_time", "fatest_lap").withColumnRenamed("time", "race_time")

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits").withColumnRenamed("name", "circuit_name").withColumnRenamed("location", "circuit_location")

# COMMAND ----------

driver_df = drivers_df.join(results_df, drivers_df.driver_id == results_df.driver_id, "inner")

# COMMAND ----------

circuit_race_df = races_df.join(circuits_df, circuits_df.circuit_id == races_df.circuit_id, "inner").select(races_df.race_id,races_df.race_year,races_df.race_name,races_df.race_date,circuits_df.circuit_location)

# COMMAND ----------

race_results_df = results_df.join(circuit_race_df, circuit_race_df.race_id == results_df.race_id, "inner")\
    .join(drivers_df, results_df.driver_id == drivers_df.driver_id, "inner")\
        .join(constructors_df, results_df.constructor_id == constructors_df.constructor_id, "inner")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df = race_results_df.select("race_year", "race_name", "race_date", "circuit_location", "driver_name", "driver_number", "driver_nationality", "team", "grid", "fastest_lap", "race_time", "points").withColumn("created_date", current_timestamp())

# COMMAND ----------

display(final_df.filter("race_year = 2020 and race_name = 'Abu Dhabi Grand Prix'").orderBy(final_df.points.desc()))

# COMMAND ----------

final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

demo_df = race_results_df.filter("race_year == 2020")

# COMMAND ----------

display(demo_df)

# COMMAND ----------

from pyspark.sql.functions import count, countDistinct, sum

# COMMAND ----------

demo_df.select(count("race_name")).show()

# COMMAND ----------

demo_df.select(countDistinct("race_name")).show()

# COMMAND ----------

demo_df.filter("driver_name ='Lewis Hamilton'").select(sum("points"), countDistinct("race_name")).show()

# COMMAND ----------

demo_df.filter("driver_name = 'Lewis Hamilton'").select(sum("points"),countDistinct("race_name")).withColumnRenamed("sum(points)", "points").withColumnRenamed("count(Distinct race_name)", "race_name").show()

# COMMAND ----------

demo_df.groupBy("driver_name").agg(sum("points").alias("points"),countDistinct("race_name").alias("race_name")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Window functions

# COMMAND ----------

demo_df = final_df.filter("race_year in (2019, 2020)")

# COMMAND ----------

display(demo_df)

# COMMAND ----------

demo_group_df = demo_df.groupBy("race_year", "driver_name").agg(sum("points").alias("points"), countDistinct("race_name").alias("race_name"))

# COMMAND ----------

display(demo_group_df)

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql.functions import desc, rank

# COMMAND ----------

driverRankSpec = Window.partitionBy("race_year").orderBy(desc("points"))
demo_group_df.withColumn("rank", rank().over(driverRankSpec)).show(100)

# COMMAND ----------


