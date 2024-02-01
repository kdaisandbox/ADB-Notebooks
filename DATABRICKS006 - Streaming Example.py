# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC create or replace table parts
# MAGIC as
# MAGIC select * from samples.tpch.part

# COMMAND ----------

(spark.readStream
      .table("parts")
      .createOrReplaceTempView("vw_parts")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from vw_parts

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select
# MAGIC   p_type, p_container, count(1) as total
# MAGIC from vw_parts
# MAGIC group by p_type, p_container

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create or replace temp view vw_part_counts
# MAGIC as
# MAGIC select
# MAGIC   p_type, p_container, count(1) as total
# MAGIC from vw_parts
# MAGIC group by p_type, p_container

# COMMAND ----------

(spark.table("vw_part_counts")                               
      .writeStream  
      .trigger(processingTime='1 seconds')
      .outputMode("complete")
      .option("checkpointLocation", "dbfs:/mnt/demo/part_count_checkpoint")
      .table("part_counts")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from part_counts

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC insert into parts
# MAGIC values (999999, 'DENEME1', 'Manufacturer#1', 'Brand#32', 'LARGE ANODIZED NICKEL', 1, 'WRAP CASE', 9999, 'no comment')
# MAGIC   , (999998, 'DENEME2', 'Manufacturer#1', 'Brand#32', 'LARGE ANODIZED NICKEL', 1, 'WRAP CASE', 9999, 'no comment')
# MAGIC   , (999997, 'DENEME3', 'Manufacturer#1', 'Brand#32', 'LARGE ANODIZED NICKEL', 1, 'WRAP CASE', 9999, 'no comment')
