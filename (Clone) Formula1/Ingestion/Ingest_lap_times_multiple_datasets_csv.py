# Databricks notebook source
from pyspark.sql.types import StructType, IntegerType, StructField, StringType

# COMMAND ----------

lap_times_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)])

# COMMAND ----------

# MAGIC %md
# MAGIC #Below code (4) gives whole folder to read. For specific file to read instead of folder
# MAGIC -> lap_times_df = spark.read.schema(lap_times_schema).csv("/mnt/formula1lake8/raw/lap_times/lap_times_split_*(number).csv")

# COMMAND ----------

lap_times_df = spark.read.schema(lap_times_schema).csv("/mnt/formula1lake8/raw/lap_times")

# COMMAND ----------

display(lap_times_df)

# COMMAND ----------

lap_times_df.count()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_lap_times_df = lap_times_df.withColumnRenamed("raceId", "race_id").withColumnRenamed("driverId", "driver_id").withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

display(final_lap_times_df)

# COMMAND ----------

final_lap_times_df.write.mode("overwrite").parquet("/mnt/formula1lake8/processed/lap_times")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls
# MAGIC /mnt/formula1lake8/processed/lap_times

# COMMAND ----------


