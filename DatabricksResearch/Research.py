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

# COMMAND ----------

s3_bucket = "aws-data-migration"
mount_name = "/mnt/aws-data-migration"
source_uri = 's3n://{}:{}@{}'.format(access_key,edcoded_secret_key,s3_bucket)
dbutils.fs.mount(source_uri,mount_name)

# COMMAND ----------

dbutils.fs.ls("/mnt/aws-data-migration")

# COMMAND ----------

# MAGIC %md Create Secrets

# COMMAND ----------

# # Configure Databricks CLI

# 1. Go to Command Prompt - Make sure Python3 and above installed
# 2. Type "pip install databricks-cli"
# 3. Type the below commands after the Installation
# 4. databricks configure --token
# 5. Paste the databricks url till .com/.net/etc
# 6. For the Access Token go to User Setting/Access Token and generate the token

# Creating Scope
databricks secrets create-scope --scope awsConfig
# Listing a scope
databricks secrets list-scope
# Creating a secret
databricks secrets put --scope awsConfig --key accesskey


# COMMAND ----------

access_key = dbutils.secrets.get("awsConfig" , "accesskey")

# COMMAND ----------

for char in access_key:
    print(char, end=" ")
