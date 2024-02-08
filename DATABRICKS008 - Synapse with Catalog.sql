-- Databricks notebook source
select * from synapse_katalog.dbo.factresellersales limit 100

-- COMMAND ----------

create table if not exists katalog.sema.sales_from_synapse
as
select * from synapse_katalog.dbo.factresellersales limit 100

-- COMMAND ----------

select * from katalog.sema.sales_from_synapse
