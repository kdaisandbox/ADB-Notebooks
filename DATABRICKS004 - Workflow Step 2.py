# Databricks notebook source
df = spark.sql('select * from hive_metastore.default.customer_building')

# COMMAND ----------

df.write.mode("overwrite").parquet("/tmp/customer_building_output.parquet")

# COMMAND ----------

dbutils.fs.rm("dbfs:/tmp/customer_building_output.parquet", True)

# COMMAND ----------

# MAGIC %fs
# MAGIC
# MAGIC ls "dbfs:/tmp"
