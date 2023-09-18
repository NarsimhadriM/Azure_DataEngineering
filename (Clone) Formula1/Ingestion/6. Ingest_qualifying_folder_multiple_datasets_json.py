# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

qualifying_schema = StructType(fields=[StructField("qualifyId", IntegerType(), False),
                                       StructField("raceId", IntegerType(), True),
                                       StructField("driverId", IntegerType(), True),
                                       StructField("constructorId", IntegerType(), True),
                                       StructField("number", IntegerType(), True),
                                       StructField("position", IntegerType(), True),
                                       StructField("q1", StringType(), True),
                                       StructField("q2", StringType(), True),
                                       StructField("q3", StringType(), True)])

# COMMAND ----------

qualifying_df = spark.read.schema(qualifying_schema).option("multiLine", True).json("/mnt/formula1lake8/raw/qualifying")

# COMMAND ----------

display(qualifying_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #Rename

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

qualifying_df_rename = qualifying_df.withColumnRenamed("qualifyId", "qualify_id").withColumnRenamed("raceId", "race_id").withColumnRenamed("driverId", "driver_id").withColumnRenamed("constructorId", "constructor_id").withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

display(qualifying_df_rename)

# COMMAND ----------

final_qualifying_df = qualifying_df_rename

# COMMAND ----------

final_qualifying_df.write.mode("overwrite").parquet("/mnt/formula1lake8/processed/qualifying")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls
# MAGIC /mnt/formula1lake8/processed/qualifying

# COMMAND ----------

display(spark.read.parquet("/mnt/formula1lake8/processed/qualifying"))

# COMMAND ----------


