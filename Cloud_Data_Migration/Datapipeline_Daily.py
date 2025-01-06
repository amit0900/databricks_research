# Databricks notebook source
import pyspark.sql.functions as fx

# COMMAND ----------

azureDataReadIncr = spark.read.format("csv").option("header","true").option("sep",",").load("/mnt/azureData/SourcePath").withColumn("Active_flag",fx.lit("Y"))
actveEmpFil = azureDataReadIncr.filter(fx.col("Salary")>5000)

# COMMAND ----------

actveEmpFil.createOrReplaceTempView("azure_data")

# COMMAND ----------

actveEmpFil.createOrReplaceTempView("azure_data_frame")

# COMMAND ----------

print(actveEmpFil.count())

# COMMAND ----------

awsDataRead = spark.read.format("delta").load("/mnt/aws-data-migration/deltaresult")

# COMMAND ----------

print(awsDataRead.count())

# COMMAND ----------

awsDataRead.createOrReplaceTempView("aws_data")

# COMMAND ----------

sqlquery = """(select * from azure_data minus select * from aws_data)"""

# COMMAND ----------

incr_data = spark.sql(sqlquery)

# COMMAND ----------

finalData = incr_data.distinct()

# COMMAND ----------

print(finalData.count())

# COMMAND ----------

finalData.write.mode("append").format("delta").save("/mnt/aws-data-migration/deltaresult")
