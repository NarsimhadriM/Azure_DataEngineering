# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType

# COMMAND ----------

# MAGIC %md
# MAGIC ##Schema Creation

# COMMAND ----------

dbutils.widgets.text("data_source", "")
input_data_source = dbutils.widgets.get("data_source")

# COMMAND ----------

races_schema = StructType(fields=[StructField("raceId",IntegerType(), False),
                                     StructField("year", IntegerType(), True),
                                     StructField("round", IntegerType(), True),
                                     StructField("circuitId", IntegerType(), True),
                                     StructField("name", StringType(), True),
                                     StructField("date", DateType(), True),
                                     StructField("time", StringType(), True),
                                     StructField("url", StringType(), True)

                                     ])

# COMMAND ----------

races_df = spark.read.option("header", True).schema(races_schema).csv("/mnt/formula1lake8/raw/races.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Add Ingestion date and race_timestamp

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp, concat, lit, col

# COMMAND ----------

races_with_timestamp = races_df.withColumn("ingestion_date", current_timestamp())\
    .withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss')).withColumn("data_source", lit(input_data_source))

# COMMAND ----------

# MAGIC %md
# MAGIC #Select required columns

# COMMAND ----------

races_selected_df = races_with_timestamp.select(col('raceId').alias('race_id'),col('year').alias('race_year'),col('circuitId').alias('circuit_id'),col('round'),col('name'),col('ingestion_date'),col('race_timestamp'),col('data_source'))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Write the data into ADLS in a parquet format

# COMMAND ----------

races_selected_df.write.mode('overwrite').partitionBy('race_year').parquet('/mnt/formula1lake8/processed/races')

# COMMAND ----------

display(spark.read.parquet("/mnt/formula1lake8/processed/races"))

# COMMAND ----------

dbutils.notebook.exit(" done")

# COMMAND ----------


