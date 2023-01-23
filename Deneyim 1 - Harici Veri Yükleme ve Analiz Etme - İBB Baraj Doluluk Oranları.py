# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Deneyim 1 - Harici Veri Yükleme ve Analiz Etme - İBB Baraj Doluluk Oranları

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
