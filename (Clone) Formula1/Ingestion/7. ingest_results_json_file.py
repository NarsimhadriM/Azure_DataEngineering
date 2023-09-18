# Databricks notebook source
from pyspark.sql.types import StructType, StringType, FloatType, IntegerType, DateType, StructField

# COMMAND ----------

# MAGIC %md
# MAGIC #Schema creation to read the data

# COMMAND ----------

results_schema = StructType(fields=[StructField("resultId", IntegerType(), False),
                                    StructField("raceId", IntegerType(), True),
                                    StructField("driverId", IntegerType(), True),
                                    StructField("constructorId", IntegerType(), True),
                                    StructField("number", IntegerType(), True),
                                    StructField("grid", IntegerType(), True),
                                    StructField("position", IntegerType(), True),
                                    StructField("positionText", StringType(), True),
                                    StructField("points", FloatType(), True),
                                    StructField("laps", IntegerType(), True),
                                    StructField("time", StringType(), True),
                                    StructField("milliseconds", IntegerType(), True),
                                    StructField("fastestLap", IntegerType(), True),
                                    StructField("rank", IntegerType(), True),
                                    StructField("fastestLapTime", StringType(), True),
                                    StructField("fastestLapSpeed", FloatType(), True),
                                    StructField("statusId", IntegerType(), True)])

# COMMAND ----------

results_df = spark.read.schema(results_schema).json("/mnt/formula1lake8/raw/results.json")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

results_renamed_df = results_df.withColumnRenamed("resultId", "result_id").withColumnRenamed("raceId", "race_id").withColumnRenamed("driverId", "driver_id").withColumnRenamed("constructorId", "constructor_id").withColumnRenamed("positionText", "position_text").withColumnRenamed("fastestLapTime", "fastest_lap_time").withColumnRenamed("fastestLap", "fastest_lap").withColumnRenamed("fastestLapSpeed", "fastest_lap_speed").withColumnRenamed("statusId", "status_id").withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

results_final_df = results_renamed_df.drop("status_id")

# COMMAND ----------

display(results_final_df)

# COMMAND ----------

results_final_df.write.mode("overwrite").partitionBy('race_id').parquet("/mnt/formula1lake8/processed/results")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls
# MAGIC /mnt/formula1lake8/processed/results

# COMMAND ----------

display(spark.read.parquet("/mnt/formula1lake8/processed/results"))

# COMMAND ----------


