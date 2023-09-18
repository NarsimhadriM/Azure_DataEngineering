# Databricks notebook source
from pyspark.sql.types import StringType, DateType, StructField, IntegerType, StructType, FloatType

# COMMAND ----------

# MAGIC %md
# MAGIC #Schema creation to read

# COMMAND ----------

pitstops_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                     StructField("driverId", IntegerType(), True),
                                     StructField("stop", IntegerType(), True),
                                     StructField("lap", IntegerType(), True),
                                     StructField("time", StringType(), True),
                                     StructField("duration", StringType(), True),
                                     StructField("milliseconds", StringType(), True)])

# COMMAND ----------

# MAGIC %md
# MAGIC #Read the data
# MAGIC

# COMMAND ----------

pit_stops_df = spark.read.schema(pitstops_schema).option("multiLine", True).json("/mnt/formula1lake8/raw/pit_stops.json")

# COMMAND ----------

display(pit_stops_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #Rename the fields

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_pit_stops_df = pit_stops_df.withColumnRenamed("raceId", "race_id").withColumnRenamed("driverId", "driver_id").withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

display(final_pit_stops_df)

# COMMAND ----------

final_pit_stops_df.write.mode("overwrite").parquet("/mnt/formula1lake8/processed/pit_stops")

# COMMAND ----------

display(spark.read.parquet("/mnt/formula1lake8/processed/pit_stops"))

# COMMAND ----------

# MAGIC %fs
# MAGIC ls
# MAGIC /mnt/formula1lake8/processed/pit_stops

# COMMAND ----------


