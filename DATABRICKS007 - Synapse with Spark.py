# Databricks notebook source
# MAGIC %md
# MAGIC #SPARK#

# COMMAND ----------

# MAGIC %md
# MAGIC ##Get some data from an Azure Synapse table - With credentials##

# COMMAND ----------

df_credentials = (spark.read
    .format("sqldw")
    .option("host", "XXXXX.database.windows.net")
    .option("port", "1433") # Optional - will use default port 1433 if not specified.
    .option("user", "XXXXX")
    .option("password", "XXXXX")
    .option("database", "XXXXX")
    .option("dbtable", "dbo.dimcustomer") # If schemaName not provided, default to "dbo".
    .option("forwardSparkAzureStorageCredentials", "true")
    .load())

display(df_credentials)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Get some data from an Azure Synapse table - With JDBC##

# COMMAND ----------

df_jdbc = spark.read \
  .format("com.databricks.spark.sqldw") \
  .option("url", "jdbc:sqlserver://XXXXX.database.windows.net:1433;database=XXXXX;user=XXXXX@XXXXX;password=XXXXX;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;") \
  .option("forwardSparkAzureStorageCredentials", "true") \
  .option("dbTable", "dimcustomer") \
  .load()

display(df_jdbc)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Load data from an Azure Synapse query##

# COMMAND ----------

df_query = spark.read \
  .format("com.databricks.spark.sqldw") \
 .option("url", "jdbc:sqlserver://XXXXX.database.windows.net:1433;database=XXXXX;user=XXXXX@XXXXX;password=XXXXX;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;") \
  .option("forwardSparkAzureStorageCredentials", "true") \
  .option("query", "select top 10 * from dimcustomer") \
  .load()

display(df_query)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Write the data back to another table in Azure Synapse##

# COMMAND ----------

df_query.write \
  .format("com.databricks.spark.sqldw") \
  .option("url", "jdbc:sqlserver://XXXXX.database.windows.net:1433;database=XXXXX;user=XXXXX@XXXXX;password=XXXXX;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;") \
  .option("forwardSparkAzureStorageCredentials", "true") \
  .option("dbTable", "deneme") \
  .save()

# COMMAND ----------

# MAGIC %md
# MAGIC #SQL#

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Set up the storage account access key in the notebook session conf.
# MAGIC SET fs.azure.account.key.<your-storage-account-name>.dfs.core.windows.net=<your-storage-account-access-key>;
# MAGIC
# MAGIC -- Read data using SQL. The following example applies to Databricks Runtime 11.3 LTS and above.
# MAGIC CREATE TABLE example_table_in_spark_read
# MAGIC USING sqldw
# MAGIC OPTIONS (
# MAGIC   host '<hostname>',
# MAGIC   port '<port>' /* Optional - will use default port 1433 if not specified. */
# MAGIC   user '<username>',
# MAGIC   password '<password>',
# MAGIC   database '<database-name>'
# MAGIC   dbtable '<schema-name>.<table-name>', /* If schemaName not provided, default to "dbo". */
# MAGIC   forwardSparkAzureStorageCredentials 'true',
# MAGIC   tempDir 'abfss://<your-container-name>@<your-storage-account-name>.dfs.core.windows.net/<your-directory-name>'
# MAGIC );
# MAGIC
# MAGIC -- Read data using SQL. The following example applies to Databricks Runtime 10.4 LTS and below.
# MAGIC CREATE TABLE example_table_in_spark_read
# MAGIC USING com.databricks.spark.sqldw
# MAGIC OPTIONS (
# MAGIC   url 'jdbc:sqlserver://<the-rest-of-the-connection-string>',
# MAGIC   forwardSparkAzureStorageCredentials 'true',
# MAGIC   dbtable '<your-table-name>',
# MAGIC   tempDir 'abfss://<your-container-name>@<your-storage-account-name>.dfs.core.windows.net/<your-directory-name>'
# MAGIC );
# MAGIC
# MAGIC -- Write data using SQL.
# MAGIC -- Create a new table, throwing an error if a table with the same name already exists:
# MAGIC
# MAGIC CREATE TABLE example_table_in_spark_write
# MAGIC USING com.databricks.spark.sqldw
# MAGIC OPTIONS (
# MAGIC   url 'jdbc:sqlserver://<the-rest-of-the-connection-string>',
# MAGIC   forwardSparkAzureStorageCredentials 'true',
# MAGIC   dbTable '<your-table-name>',
# MAGIC   tempDir 'abfss://<your-container-name>@<your-storage-account-name>.dfs.core.windows.net/<your-directory-name>'
# MAGIC )
# MAGIC AS SELECT * FROM table_to_save_in_spark;

# COMMAND ----------

# MAGIC %md
# MAGIC ##Create temp view from dataframe - Create Delta table using this temp view##

# COMMAND ----------

df_query.createOrReplaceTempView("vw_dimcustomer")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS katalog.sema.dim_customer_10
# MAGIC AS
# MAGIC SELECT * FROM vw_dimcustomer LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM katalog.sema.dim_customer_10
