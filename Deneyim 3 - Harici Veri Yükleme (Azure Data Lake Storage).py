# Databricks notebook source
# MAGIC %md
# MAGIC # Deneyim 3 - Harici Veri Yükleme (Azure Data Lake Storage)

# COMMAND ----------

# MAGIC %md
# MAGIC Bu deneyimde örnek bir veri setinin, **Azure Data Lake Storage**'tan **Databricks** dosya sistemi üzerine alınması ve **mount** edilmesi anlatılacaktır. 
# MAGIC 
# MAGIC Örnek olarak **Yıllara göre Türkiye'nin dış borç miktarları** veri seti kullanılacaktır.
# MAGIC 
# MAGIC İlk olarak AIVerse örnek veri setlerinin olduğu klasörü Databricks üzerinde **mount** edilmesi gerekir. Önce mevcut klasörler varsa onları temizlemek için **dbutils.fs.unmount()** komutu çalıştırılır.

# COMMAND ----------

if any(mount.mountPoint == "/mnt/aiverse-sample-data" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount("/mnt/aiverse-sample-data")

# COMMAND ----------

# MAGIC %md
# MAGIC AWS S3 bucket'ının adı ve mount edilecek klasör adlarıyla S3 klasör yolu oluşturulur ve **dbutils.fs.mount()** komutu içine bu yol verilerek yeni mount işlemi yapılır.

# COMMAND ----------

aws_bucket_name = "aiverse-sample-data-files"
mount_name = "aiverse-sample-data"
dbutils.fs.mount("s3a://%s" % aws_bucket_name, "/mnt/%s" % mount_name)

# COMMAND ----------

# MAGIC %md
# MAGIC Mevcuttaki bütün mount bilgilerini görmek için **dbutils.fs.mounts()** komutuyla kontrol yapılabilir.

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

# MAGIC %md
# MAGIC Mount edilen S3 klasörü altındaki **CSV** alt klasöründe yer alan dosyaların listesini bakmak için önce yine tam klasör yolu (full_path) parametrik olarak oluşturulur ve **dbutils.fs.ls()** komutu ile dosyalar listelenir.

# COMMAND ----------

s3_sub_folder = "csv"

full_path = "/mnt/%s/%s" % (mount_name, s3_sub_folder)

display(dbutils.fs.ls(full_path))

# COMMAND ----------

# MAGIC %md
# MAGIC Yıllara göre Türkiye'nin dış borçlarının bulunduğu **external-debt_tur.csv** dosyasını spark ile bir dataframe içine almak ve kayıtları göstermek için aşağıdaki kod çalıştırılır.

# COMMAND ----------

df = (spark.read
      .format("csv")
      .option("header", True)
      .load("%s/external-debt_tur.csv" % full_path))

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Burada dikkat edilirse ilk satırda standart dışı değerler geldiği görülecektir. **.option("header", True)** parametresi kullanılarak ilk satırın başlık satırı olduğu belirtilse de dosyada fazladan bir satır daha bulunmaktadır.
# MAGIC 
# MAGIC Bu satırı atlayıp diğer satırdan devam etmek için **.option("skipRows", 1)** parametresi **spark.read** metoduna eklenirse sorunun düzeldiği görülecektir.

# COMMAND ----------

df = (spark.read
      .format("csv")
      .option("header", True)
      .option("skipRows", 1)
      .load("%s/external-debt_tur.csv" % full_path))

display(df)
