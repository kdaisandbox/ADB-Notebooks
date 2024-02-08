# Databricks notebook source
df = spark.sql('select * from hive_metastore.default.customer_building')

# COMMAND ----------

df.write.mode("overwrite").parquet("/tmp/customer_building_output.parquet")

# COMMAND ----------

# MAGIC %fs
# MAGIC
# MAGIC ls "dbfs:/tmp/customer_building_output.parquet"

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create or replace table catalog_1.test.customer_building_table
# MAGIC as
# MAGIC select *
# MAGIC from hive_metastore.default.customer_building
