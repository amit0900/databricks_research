# Databricks notebook source
import pyspark.sql.functions as fx
import urllib

# COMMAND ----------

# cred = spark.read.format("csv").option("header","true").option("sep",",").load("/FileStore/tables/credentials")
# cred = cred.withColumnRenamed("User name","username").withColumnRenamed("Access key ID","access_key").withColumnRenamed("Secret access key","secret_key")

# COMMAND ----------

display(cred)

# COMMAND ----------

access_key = dbutils.secrets.get("awsSecretAccess","accesskey")
secret_key = dbutils.secrets.get("awsSecretAccess","secretkey")

# COMMAND ----------

# for char in secret_key:
#     print(char,end=" ")

# COMMAND ----------

import urllib
secret_key_encod = urllib.parse.quote(secret_key,"")

# COMMAND ----------

s3bucket = "aws-data-analytics-mgt"
mount_name = "/mnt/"+s3bucket
source_uri = 's3n://{}:{}@{}'.format(access_key,secret_key_encod,s3bucket)

# COMMAND ----------

dbutils.fs.mount(source_uri,mount_name)

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

order_data = spark.read.format("csv").option("header","true").option("sep",",").load("/mnt/aws-data-analytics-mgt/Dataset")

# COMMAND ----------

display(order_data)

# COMMAND ----------

print(mount_name)

# COMMAND ----------

dbutils.fs.mkdirs("/FileStore/tables/data")

# COMMAND ----------

dbutils.fs.ls("/mnt/aws-data-migration")

# COMMAND ----------

order_data = spark.read.format("csv").option("header","true").option("sep",",").load("/FileStore/tables/data")
order_data = order_data.withColumnRenamed(" order_date","order_date").withColumnRenamed(" order_cust_id","order_cust_id").withColumnRenamed(" order_status","order_status")

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables/data

# COMMAND ----------

order_data.count()

# COMMAND ----------

order_data.coalesce(1).write.format("delta").mode("overwrite").save("/mnt/aws-data-migration/Target")

# COMMAND ----------

tarData = spark.read.format("delta").load("/mnt/aws-data-migration/Target")

# COMMAND ----------

tarData.persist(StorageLevel.MEMORY_AND_DISK)

# COMMAND ----------

tarData.count()

# COMMAND ----------

tarData.repartition(10).write.format("csv").save("/mnt/aws-data-migration/Target/repart")

# COMMAND ----------

tarData.display()

# COMMAND ----------

tarData = spark.read.format("delta").load("/mnt/aws-data-migration/Target/")

# COMMAND ----------

tarData = spark.read.format("csv").load("/mnt/aws-data-analytics-mgt/Dataset/")

# COMMAND ----------

dbutils.fs.mounts()
