# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Deneyim 2 - Harici Veri Yükleme (AWS S3)

# COMMAND ----------

# MAGIC %md
# MAGIC Bu deneyimde örnek bir veri setinin, AWS S3 bucket'tan Databricks dosya sistemi üzerine alınması ve mount edilmesi anlatılacaktır. 
# MAGIC 
# MAGIC Örnek olarak **Yıllara göre Türkiye'nin dış borç miktarları** veri seti kullanılacaktır.

# COMMAND ----------

dbutils.fs.unmount("/mnt/aiverse-sample-data")

# COMMAND ----------

aws_bucket_name = "aiverse-sample-data-files"
mount_name = "aiverse-sample-data"
dbutils.fs.mount("s3a://%s" % aws_bucket_name, "/mnt/%s" % mount_name)

# COMMAND ----------

s3_sub_folder = "csv"

full_path = "/mnt/%s/%s" % (mount_name, s3_sub_folder)

display(dbutils.fs.ls(full_path))

# COMMAND ----------

df = spark.read.format("csv").option("header", True).load("%s/external-debt_tur.csv" % full_path)

display(df)
