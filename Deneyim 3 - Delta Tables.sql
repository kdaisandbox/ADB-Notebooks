-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Deneyim 3 - Delta Tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Bu deneyimde Databricks'te **Delta tables** ile ilgili örnek senaryolar bulunmaktadır.
-- MAGIC 
-- MAGIC Örnekler **SQL** ile yazılacağından notebook'un varsayılan dili olarak **SQL** seçilmiştir.
-- MAGIC 
-- MAGIC <p>
-- MAGIC <img src="https://raw.githubusercontent.com/kdaisandbox/ADB-Notebooks/main/img/D3-001.png" />
-- MAGIC </p>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Tablo oluşturma, kayıt ekleme ve sorgulama ###

-- COMMAND ----------

-- MAGIC %md
-- MAGIC aşağıdaki kolonlara sahip **employees** adında bir tablo oluşturulur:
-- MAGIC <br /><br />
-- MAGIC <ul>
-- MAGIC   <li>employee_id</li>
-- MAGIC   <li>first_name</li>
-- MAGIC   <li>last_name</li>
-- MAGIC   <li>email</li>
-- MAGIC   <li>phone_number</li>
-- MAGIC   <li>hire_date</li>
-- MAGIC   <li>job_id</li>
-- MAGIC   <li>salary,manager_id</li>
-- MAGIC   <li>department_id</li>
-- MAGIC </ul>

-- COMMAND ----------

CREATE TABLE employees (
	employee_id int
	, first_name string
	, last_name string
	, email string
	, phone_number string
	, hire_date date
	, job_id string
	, salary double
	, manager_id int
	, department_id int
)
