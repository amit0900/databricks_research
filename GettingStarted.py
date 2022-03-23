# Databricks notebook source
import pyspark.sql.functions as fx

# COMMAND ----------

order_data = spark.read.format("csv").option("header" , "true").load("dbfs:/FileStore/tables/retail_db/orders/part_00000.txt")

# COMMAND ----------

order_data.printSchema()

# COMMAND ----------

order_data = order_data.withColumnRenamed(" order_date" , "order_date").withColumnRenamed(" order_cust_id" , "order_cust_id").withColumnRenamed(" order_status" , "order_status")

# COMMAND ----------

order_data = order_data.withColumn("Kimberley" , fx.lit("Null"))

# COMMAND ----------

order_data_opt = order_data.withColumn("Kimberley" , fx.lit("Null"))

# COMMAND ----------

order_data.write.parquet("dbfs:/FileStore/tables/nvl_parq")

# COMMAND ----------

dbutils.fs.rm("dbfs:/FileStore/tables/nvl_parq",True)

# COMMAND ----------

# MAGIC %fs ls /FileStore/tables/nvl_output

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables/nvl_parq

# COMMAND ----------

order_data.printSchema()

# COMMAND ----------

order_data.display()

# COMMAND ----------

order_item_data = spark.read.format("csv").option("header" , "true").load("dbfs:/FileStore/tables/retail_db/order_items")
order_item_data = order_item_data.withColumnRenamed("order_item_order_id" , "order_id")

# COMMAND ----------

order_item_data.printSchema()

# COMMAND ----------

joinedDF = order_data.join(order_item_data , ['order_id'])

# COMMAND ----------

print(joinedDF.count())

# COMMAND ----------

joinedDF.display()

# COMMAND ----------

order_item_data.display()

# COMMAND ----------

order_item_data.display()

# COMMAND ----------

order_data.printSchema()

# COMMAND ----------

dataHeader.show()

# COMMAND ----------

data = spark.read.csv("/FileStore/tables/orders" , schema = "order_id INT , order_date STRING , order_cust_id INT , order_status STRING")

# COMMAND ----------

data.display()

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables/order_items

# COMMAND ----------

dbutils.fs.ls("dbfs:/FileStore/tables/retail_db/orders/")

# COMMAND ----------

data = spark.read.csv("dbfs:/FileStore/tables/retail_db/orders/part_00000.txt" , schema = "order_id INT , order_date STRING , order_cust_id INT , order_status STRING")

# COMMAND ----------

data.printSchema()

# COMMAND ----------

display(data)

# COMMAND ----------



# COMMAND ----------

display(dataHeader)

# COMMAND ----------

import pyspark.sql.functions as fx

# COMMAND ----------

data = dataHeader.groupBy(fx.col(" order_date")).agg(fx.min(fx.col("order_id")).alias("Order_count"))
display(data)

# COMMAND ----------

dataHeaderFil = dataHeader.filter(fx.col(" order_status") == fx.lit("CLOSED"))

# COMMAND ----------

dataHeaderFil.registerTempTable("orderstemp")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from orderstemp

# COMMAND ----------

dataHeaderFil.write.csv("dbfs:/FileStore/tables/order_output")

# COMMAND ----------

dataHeader = spark.read.format("csv").option("header" , "true").load("dbfs:/FileStore/tables/order_output")

# COMMAND ----------

display(dataHeader)
