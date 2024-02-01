# Databricks notebook source
# MAGIC %fs
# MAGIC
# MAGIC ls /databricks-datasets/retail-org

# COMMAND ----------

customers = spark.read.csv("dbfs:/databricks-datasets/retail-org/customers/", header = True)

# COMMAND ----------

customers.write.format("delta").mode("overwrite").saveAsTable("catalog_1.test.customers_bronze")
