-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Deneyim 3 - Databricks Lakehouse Platform

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Bu deneyimde Databricks Lakehouse platformu üzerinde; **Delta tabloları** ile CRUD (Create - Select (Read) - Update - Delete) işlemleri, tabloların versiyonlanması, eski versiyonlara dönülmesi ve optimizasyon gibi örnekler içermektedir.
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
-- MAGIC Örneklerde kullanılmak üzere, aşağıdaki kolonlara sahip **employees** adında bir tablo oluşturulur:
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

DROP TABLE IF EXISTS employees;

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
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Employees tablosuna örnek kayıtlar eklenir.

-- COMMAND ----------

INSERT INTO employees
VALUES
  (198,'Donald','OConnell','DOCONNEL',6505079833,'2007-06-01','SH_CLERK',2600,124,50),
  (199,'Douglas','Grant','DGRANT',6505079844,'2008-01-13','SH_CLERK',2600,124,50),
  (200,'Jennifer','Whalen','JWHALEN',5151234444,'2003-09-17','AD_ASST',4400,101,10),
  (201,'Michael','Hartstein','MHARTSTE',5151235555,'2004-02-17','MK_MAN',13000,100,20),
  (202,'Pat','Fay','PFAY',6031236666,'2005-08-17','MK_REP',6000,201,20),
  (203,'Susan','Mavris','SMAVRIS',5151237777,'2002-06-07','HR_REP',6500,101,40),
  (204,'Hermann','Baer','HBAER',5151238888,'2002-06-07','PR_REP',10000,101,70),
  (205,'Shelley','Higgins','SHIGGINS',5151238080,'2002-06-07','AC_MGR',12008,101,110),
  (206,'William','Gietz','WGIETZ',5151238181,'2002-06-07','AC_ACCOUNT',8300,205,110),
  (100,'Steven','King','SKING',5151234567,'2003-06-17','AD_PRES',2400,0,90),
  (101,'Neena','Kochhar','NKOCHHAR',5151234568,'2005-09-21','AD_VP',17000,100,90),
  (102,'Lex','De Haan','LDEHAAN',5151234569,'2001-01-13','AD_VP',17000,100,90),
  (103,'Alexander','Hunold','AHUNOLD',5904234567,'2006-01-03','IT_PROG',9000,102,60),
  (104,'Bruce','Ernst','BERNST',5904234568,'2007-05-21','IT_PROG',6000,103,60),
  (105,'David','Austin','DAUSTIN',5904234569,'2005-06-25','IT_PROG',4800,103,60),
  (106,'Valli','Pataballa','VPATABAL',5904234560,'2006-02-05','IT_PROG',4800,103,60)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Eklenen kayıtlar kontrol edilir.

-- COMMAND ----------

select * from employees

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Tablo detayları incelenirse, tablo formatının **delta** olduğu ve varsayılan konumun da **dbfs:/user/hive/warehouse/metastore** olduğu görülür.

-- COMMAND ----------

DESCRIBE DETAIL employees

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Tabloya ait dosyaların listesini görmek için **%fs** magic command'i kullanılarak **employees** klasörü altındaki dosyalar listelenir.

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC ls 'dbfs:/user/hive/warehouse/employees'
