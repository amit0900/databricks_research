# Databricks notebook source
import pyspark.sql.functions as fx

# COMMAND ----------

# MAGIC %md ## Data Ingestion

# COMMAND ----------

azureDataRead = spark.read.format("csv").option("header","true").option("sep",",").load("/mnt/azureData/SourcePath")

# COMMAND ----------

# MAGIC %md ## Data Transformation

# COMMAND ----------

actveEmpFlg = azureDataRead.withColumn("Active_flag",fx.lit("Y"))

# COMMAND ----------

actveEmpFilter = actveEmpFlg.filter(fx.col("Salary")>5000)

# COMMAND ----------

# MAGIC %md ## Data Load Layer

# COMMAND ----------

actveEmpFilter.write.mode("append").format("delta").save("/mnt/aws-data-migration/deltaresult")
