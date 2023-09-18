# Databricks notebook source
constructors_schema = "constructorId int, constructorRef string, name string, nationality string, url string"

# COMMAND ----------

constructor_df = spark.read.schema(constructors_schema).json("/mnt/formula1lake8/raw/constructors.json")

# COMMAND ----------

display(constructor_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #drop unwanted column instead of select required columns.

# COMMAND ----------

constructor_dropped_df = constructor_df.drop('url')

# COMMAND ----------

display(constructor_dropped_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #Ingestion Date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

constructor_final_df = constructor_dropped_df.withColumnRenamed("constructorId", "constructor_id")\
    .withColumnRenamed("constructorRef", "constructor_ref")\
        .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

display(constructor_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #Write data to parquet file

# COMMAND ----------

constructor_final_df.write.mode('overwrite').parquet("/mnt/formula1lake8/processed/constructors")

# COMMAND ----------

spark.read.parquet("/mnt/formula1lake8/processed/constructors")

# COMMAND ----------


