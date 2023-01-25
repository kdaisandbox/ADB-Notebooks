# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Deneyim 1 - Harici Veri Yükleme ve Analiz Etme

# COMMAND ----------

# MAGIC %md
# MAGIC Bu deneyimde örnek bir veri setinin, internet üzerinden (açık paylaşım bağlantısı ile) Databricks dosya sistemi üzerine alınması ve hem PySpark hem de Databricks SQL kullanarak bu verinin sorgulanması işlemleri anlatılacaktır.
# MAGIC 
# MAGIC Örnek olarak **İBB baraj doluluk oranları** veri seti kullanılacaktır.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Veriler için yeni klasör oluşturulması
# MAGIC 
# MAGIC Varsayılan klasör (default Hive metastore) içinde yeni bir klasör oluşturulur.

# COMMAND ----------

import datetime
currentdate = datetime.datetime.now().strftime("%Y-%m-%d")    
new_folder = 'data-' + currentdate

dbutils.fs.mkdirs("user/hive/warehouse/" + new_folder)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Klasörün oluştuğu kontrol edilir.

# COMMAND ----------

# MAGIC %fs
# MAGIC 
# MAGIC ls "user/hive/warehouse"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC AIVerse Github repository'sinden baraj doluluk oranları veri seti dosya sistemi içindeki **/tmp** klasörüne yüklenir.

# COMMAND ----------

# MAGIC %sh
# MAGIC 
# MAGIC wget -P /tmp https://raw.githubusercontent.com/kdaisandbox/sample-data/main/dam_occupancy.csv

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC tmp klasörü içinde dosya kontrol edilir.

# COMMAND ----------

# MAGIC %sh
# MAGIC 
# MAGIC ls /tmp | grep dam_

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Geçici klasöre indirilen dosya varsayılan dosya sistemi içinde yeni oluşturulan klasöre kopyalanır.

# COMMAND ----------

dbutils.fs.cp("file:///tmp/dam_occupancy.csv", "user/hive/warehouse/" + new_folder)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Veri setinin klasöre doğru şekilde geldiği kontrol edilir.

# COMMAND ----------

display(dbutils.fs.ls("user/hive/warehouse/" + new_folder))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Veri setinin tam yolu, bundan sonraki işlemler için kolaylık olması açısından bir değişkene alınır.

# COMMAND ----------

dosya_adi = "/user/hive/warehouse/" + new_folder + "/dam_occupancy.csv"

spark.conf.set('dosya.adi', dosya_adi)

print(dosya_adi)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### PySpark

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

sema = StructType([
    StructField("tarih", DateType(), True),
    StructField("ORT_DOLULUK_ORANI", FloatType(), True),
    StructField("ORT_KALAN", FloatType(), True)
])

df = (
        spark.read
            .format("csv")
            .option("header", "true")
            .schema(sema)
            .load(dosya_adi)
)

display(df)

# COMMAND ----------

df2 = df \
    .withColumn("YIL", date_format("tarih", "yyyy")) \
    .groupBy("YIL") \
    .avg("ORT_DOLULUK_ORANI", "ORT_KALAN") \
    .orderBy("YIL")

display(df2)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Visualization aracı kullanılarak gelen sonuçlar görselleştirme
# MAGIC 
# MAGIC Yukarıdaki sonuç hücresinde, tablonun üzerinde yer alan Table sekmesinin yanındaki + işaretine ve sonrasında **Visualization** seçeneğine tıklanır.
# MAGIC 
# MAGIC <br /><img src ='https://github.com/kdaisandbox/ADB-Notebooks/blob/main/img/D1-001.png?raw=true'><br /><br />
# MAGIC 
# MAGIC Visualization type **Bar** olarak seçilir. X kolonu **YIL** ve Y kolonları, **Add column** tıklanarak sırasıyla **ORT_DOLULUK_ORANI** ve **ORT_KALAN** olarak seçilir. 
# MAGIC 
# MAGIC <br /><img src ='https://github.com/kdaisandbox/ADB-Notebooks/blob/main/img/D1-002.png?raw=true'><br /><br />
# MAGIC 
# MAGIC **Save**'e tıklandığında hücrenin durumu aşağıdaki gibi olmalıdır:
# MAGIC 
# MAGIC <br /><img src ='https://github.com/kdaisandbox/ADB-Notebooks/blob/main/img/D1-003.png?raw=true'>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### SQL
# MAGIC 
# MAGIC Öncelikle yukarıda indirilen **dam_occupancy.csv** dosyası kullanılarak **ibb_baraj_sql** adında bir tablo oluşturulur.
# MAGIC 
# MAGIC **%sql** magic command'i kullanılarak yazılacak kodun SQL dilinde olduğu belirtilir.
# MAGIC 
# MAGIC Sonrasında **CREATE TABLE** ifadesine, başlık ve kolon ayracı parametreleri ve dosya yolunu belirten **LOCATION** parametreleri ile yeni tablo oluşturulur.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS ibb_baraj_sql
# MAGIC   (
# MAGIC     DATE date
# MAGIC     , GENERAL_DAM_OCCUPANCY_RATE float
# MAGIC     , GENERAL_DAM_RESERVED_WATER float
# MAGIC   )
# MAGIC USING CSV
# MAGIC OPTIONS (
# MAGIC   header = "true",
# MAGIC   delimiter = ","
# MAGIC )
# MAGIC LOCATION "${dosya.adi}"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Oluşan tablonun içindeki veriler sorgulanır.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM ibb_baraj_sql

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Yıllara göre ortalama doluluk oranı ve kalan su rezervini veren sorgu sonucu **vw_ibb_baraj_sql** adında bir **temporary view** içine atılır.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE OR REPLACE TEMP VIEW vw_ibb_baraj_sql
# MAGIC AS
# MAGIC SELECT 
# MAGIC   YEAR(DATE) AS YIL
# MAGIC   , AVG(GENERAL_DAM_OCCUPANCY_RATE) AS ORT_DOLULUK_ORANI
# MAGIC   , AVG(GENERAL_DAM_RESERVED_WATER) AS ORT_KALAN
# MAGIC FROM
# MAGIC   ibb_baraj_sql
# MAGIC GROUP BY
# MAGIC   YIL
# MAGIC ORDER BY 
# MAGIC   YIL

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from vw_ibb_baraj_sql

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Visualization aracı kullanılarak gelen sonuçlar görselleştirme
# MAGIC 
# MAGIC Yukarıdaki sonuç hücresinde, tablonun üzerinde yer alan Table sekmesinin yanındaki + işaretine ve sonrasında **Visualization** seçeneğine tıklanır.
# MAGIC 
# MAGIC <br /><img src ='https://github.com/kdaisandbox/ADB-Notebooks/blob/main/img/D1-001.png?raw=true'><br /><br />
# MAGIC 
# MAGIC Visualization type **Bar** olarak seçilir. X kolonu **YIL** ve Y kolonları, **Add column** tıklanarak sırasıyla **ORT_DOLULUK_ORANI** ve **ORT_KALAN** olarak seçilir. 
# MAGIC 
# MAGIC <br /><img src ='https://github.com/kdaisandbox/ADB-Notebooks/blob/main/img/D1-002.png?raw=true'><br /><br />
# MAGIC 
# MAGIC **Save**'e tıklandığında hücrenin durumu aşağıdaki gibi olmalıdır:
# MAGIC 
# MAGIC <br /><img src ='https://github.com/kdaisandbox/ADB-Notebooks/blob/main/img/D1-003.png?raw=true'>
