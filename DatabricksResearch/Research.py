# Databricks notebook source
import pyspark.sql.functions as fx
import urllib

# COMMAND ----------

dbutils.fs.ls("/FileStore/tables/credentials")

# COMMAND ----------

dbutils.fs.mv("/FileStore/tables/new_user_credentials__1_.csv","/FileStore/tables/credentials")

# COMMAND ----------

# MAGIC %md Mount S3 Bucket

# COMMAND ----------

file_path = "/FileStore/tables/credentials"
file_type = "csv"
fst_row_num = "true"
delimiter = ","

cred = spark.read.format(file_type).option("header",fst_row_num).option("sep" , delimiter).load(file_path)
cred = cred.withColumnRenamed("User name","username").withColumnRenamed("Access key ID","access_key").withColumnRenamed("Secret access key","secret_key")

# COMMAND ----------

display(cred)

# COMMAND ----------

access_key = cred.filter(fx.col("username") == "S3AccessKey").select(fx.col("access_key")).collect()[0].access_key
secret_key = cred.filter(fx.col("username") == "S3AccessKey").select(fx.col("secret_key")).collect()[0].secret_key
edcoded_secret_key = urllib.parse.quote(secret_key,"")

print(edcoded_secret_key)

# COMMAND ----------

s3_bucket = "aws-data-migration"
mount_name = "/mnt/aws-data-migration"
source_uri = 's3n://{}:{}@{}'.format(access_key,edcoded_secret_key,s3_bucket)
dbutils.fs.mount(source_uri,mount_name)

# COMMAND ----------

dbutils.fs.ls(mount_name)
