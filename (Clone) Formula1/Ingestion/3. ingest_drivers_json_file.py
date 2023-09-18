# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, DateType, StringType

# COMMAND ----------

name_schema = StructType(fields=[StructField("forename", StringType(), True),
                                 StructField("surname", StringType(), True)])

# COMMAND ----------

driver_schema = StructType(fields=[StructField("driverId", IntegerType(), False),
                                   StructField("driverRef", StringType(), True),
                                   StructField("number", IntegerType(), True),
                                   StructField("code", StringType(), True),
                                   StructField("name", name_schema),
                                   StructField("dob", DateType(), True),
                                   StructField("nationality", StringType(), True),
                                   StructField("url", StringType(), True)])

# COMMAND ----------

drivers_df = spark.read.schema(driver_schema).json("/mnt/formula1lake8/raw/drivers.json")

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #Rename the columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col, lit, concat

# COMMAND ----------

drivers_renamed_df = drivers_df.withColumnRenamed("driverId", "driver_id").withColumnRenamed("driverRef", "driver_ref").withColumn("ingestion_date", current_timestamp()).withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname")))

# COMMAND ----------

display(drivers_renamed_df)

# COMMAND ----------

drivers_final_df = drivers_renamed_df.drop(col("url"))

# COMMAND ----------

drivers_final_df.write.mode("overwrite").parquet("/mnt/formula1lake8/processed/drivers")
