# Databricks notebook source
access_key = dbutils.secrets.get("scope","secret_name")
secret_key = dbutils.secrets.get("scope","secret_name")

# COMMAND ----------

spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", access_key)
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secret_key)
spark._jsc.hadoopConfiguration().set("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
spark._jsc.hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.ap-south-1.amazonaws.com")
spark.sparkContext.setSystemProperty("com.amazonaws.services.s3.enableV4", "true")
