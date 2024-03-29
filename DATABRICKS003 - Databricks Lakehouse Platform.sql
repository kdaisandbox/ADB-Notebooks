-- Databricks notebook source
-- MAGIC %md
-- MAGIC # DATABRICKS003 - Databricks Lakehouse Platform

-- COMMAND ----------

-- MAGIC %md
-- MAGIC This experience will introduce Databricks Lakehouse platform, using examples for CRUD (Create - Select (Read) - Update - Delete) operations, versioning (time travel) and optimization.
-- MAGIC 
-- MAGIC Since all examples are SQL expressions, the default language of notebook has been switched to **SQL**.
-- MAGIC <br /><br />
-- MAGIC <p>
-- MAGIC <img src="https://raw.githubusercontent.com/kdaisandbox/ADB-Notebooks/main/img/D3-001.png" />
-- MAGIC </p>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create, Insert and Select a Table ###

-- COMMAND ----------

-- MAGIC %md
-- MAGIC We'll be using an **employees** table which has following columns:
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
-- MAGIC 
-- MAGIC Here's the **CREATE** script for the table:

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
-- MAGIC Insert some records to **employees** table:

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
-- MAGIC We can query the table to see if all the records were properly inserted.

-- COMMAND ----------

SELECT * FROM employees

-- COMMAND ----------

-- MAGIC %md
-- MAGIC If we look at the details of the table, we can see that the table format is **Delta** and the default location is **dbfs:/user/hive/warehouse/metastore**. All tables created in Databricks are **Delta tables** by default and all tables were stored in **dbfs:/user/hive/warehouse/metastore** folder.

-- COMMAND ----------

DESCRIBE DETAIL employees

-- COMMAND ----------

-- MAGIC %md
-- MAGIC By using **%fs** magic command, we can use command line within the notebook. The command below lists the files under the **employees** folder.

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC ls 'dbfs:/user/hive/warehouse/employees'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC There is one **parquet** file for the table for now. Let's make some changes on the table:

-- COMMAND ----------

UPDATE employees
SET salary = salary * 2
WHERE first_name like 'D%'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Check the **salary** value has been updates for 3 records (name starting with D).

-- COMMAND ----------

SELECT * FROM employees

-- COMMAND ----------

-- MAGIC %md
-- MAGIC If we check the table files again, we can see that there's a new **parquet** file is added to the folder.

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC ls 'dbfs:/user/hive/warehouse/employees'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC List the operations that has been made on the table suing **DESCRIBE HISTORY** command and see the **UPDATE** operation as the last one.

-- COMMAND ----------

DESCRIBE HISTORY employees

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Logs of these operations resides in **_delta_log** folder. Also, a **json** file is generated for every transaction.

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC ls 'dbfs:/user/hive/warehouse/employees/_delta_log'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC By viewing the content of the last json file (**....002.json**), updated records can bee seen with **"operation":"UPDATE"** information.

-- COMMAND ----------

-- MAGIC %fs head 'dbfs:/user/hive/warehouse/employees/_delta_log/00000000000000000002.json'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Versioning (Time Travel)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC The last operation on **employees** table was **UPDATE**. If we want to rollback this operation or return to the older version of the table, we can use **time travel** feature.

-- COMMAND ----------

DESCRIBE HISTORY employees

-- COMMAND ----------

-- MAGIC %md
-- MAGIC It's possible to go back to any version (snapshot) of the table by identifying the number in **version** column. The query below brings back the first version of the table:

-- COMMAND ----------

SELECT * 
FROM employees VERSION AS OF 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC The same query can be written with **@+version**.

-- COMMAND ----------

SELECT * FROM employees@v1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Perform another operation on the table (here, it is **delete**) and check a new version is generated.

-- COMMAND ----------

DELETE FROM employees

-- COMMAND ----------

-- MAGIC %md
-- MAGIC No results returns since all rows has been deleted.

-- COMMAND ----------

SELECT * FROM employees

-- COMMAND ----------

-- MAGIC %md
-- MAGIC View table history and check **DELETE** opration is added as 3rd version.

-- COMMAND ----------

DESCRIBE HISTORY employees

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **RESTORE TABLE** command is used to return table back to the previous versions. Here's the code for going back to the version where UPDATE was performed:

-- COMMAND ----------

  RESTORE TABLE employees TO VERSION AS OF 2 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC All records will be back as it was in the previous version.

-- COMMAND ----------

SELECT * FROM employees

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **RESTORE TABLE** is also a new operation on the table. So, we can see this step as version 4 if we check the history.

-- COMMAND ----------

DESCRIBE HISTORY employees

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Optimization (Optimize, Indexing)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC If we would like to increase the performance of our queries, we can use **ZORDER BY** clause, which orders rows by the column specified, alongside **OPTIMIZE** command.
-- MAGIC 
-- MAGIC In this example, our **employees** table is not big enough to see the increase. Anyway, usage of this expression is as follows:

-- COMMAND ----------

OPTIMIZE employees
ZORDER BY employee_id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC We can check the details and history after this operation as well.

-- COMMAND ----------

DESCRIBE DETAIL employees

-- COMMAND ----------

DESCRIBE HISTORY employees

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Clean Files (Vacuum)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Açıklama

-- COMMAND ----------


