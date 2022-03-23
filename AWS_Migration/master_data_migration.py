# Databricks notebook source
# MAGIC %run /Users/amit_achary@yahoo.in/AWS_Migration/Connections

# COMMAND ----------

import psyaprk.sql.functions as fx

# COMMAND ----------

input_path = s3a://amit-gcp-data-migration/processed/
output_path = s3a://amit-gcp-data-migration/processed/target

# COMMAND ----------

dataRead = spark.read.format("csv").option('header','true').load(input_path)

# COMMAND ----------

dataRead.write.parquet(output_path)
