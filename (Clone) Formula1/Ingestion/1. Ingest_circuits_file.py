# Databricks notebook source
# MAGIC %md ###Ingest circuits.csv file

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

dbutils.widgets.text("data_source", "")
input_data_source = dbutils.widgets.get("data_source")

# COMMAND ----------

# MAGIC %md ##1.Schema Creation

# COMMAND ----------

circuits_schema = StructType(fields=[StructField("circuitId",IntegerType(), False),
                                     StructField("circuitRef", StringType(), True),
                                     StructField("name", StringType(), True),
                                     StructField("location", StringType(), True),
                                     StructField("country", StringType(), True),
                                     StructField("lat", DoubleType(), True),
                                     StructField("long", DoubleType(), True),
                                     StructField("alt", IntegerType(), True),
                                     StructField("url", StringType(), True)

                                     ])

# COMMAND ----------

circuits_df = spark.read.option("header", True)\
.schema(circuits_schema).\
csv("/mnt/formula1lake8/raw/circuits.csv")

# COMMAND ----------

circuits_select_df = circuits_df.select("circuitId", "circuitRef", "name", "location", "country", "lat", "long", "alt")

# COMMAND ----------

from pyspark.sql.functions import col, lit

# COMMAND ----------

circuits_select_df = circuits_df.select(col("circuitID"), col("circuitRef"), col("name"), col("location"), col("country"), col("lat"), col("long"), col("alt"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##2.Attributes Rename

# COMMAND ----------

circuits_column_df = circuits_select_df.withColumnRenamed("circuitID", "circuit_id").withColumnRenamed("circuitRef", "circuit_ref").withColumnRenamed("lat", "latitude").withColumnRenamed("long", "longitude").withColumnRenamed("alt", "altitude").withColumn("data_source", lit(input_data_source))

# COMMAND ----------

# MAGIC %md 
# MAGIC ##3.Create Ingestion Timestamp for audit/tracking purpose

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

circuits_final_df = circuits_column_df.withColumn("Ingestion Date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ##4.Create data in a Parquet format and write in the ADLS

# COMMAND ----------

circuits_final_df.write.mode("overwrite").parquet("/mnt/formula1lake8/processed/circuits")

# COMMAND ----------

df = spark.read.parquet("/mnt/formula1lake8/processed/circuits")

# COMMAND ----------

display(spark.read.parquet("/mnt/formula1lake8/processed/circuits"))

# COMMAND ----------

dbutils.notebook.exit("Job Complete")

# COMMAND ----------


