-- Databricks notebook source
CREATE OR REPLACE TABLE hive_metastore.default.customer_building
AS
SELECT
  c.c_name AS CustomerName
  , n.n_name AS Nation
  , c.c_comment AS Comment
FROM samples.tpch.customer c
LEFT JOIN samples.tpch.nation n
  ON c.c_nationkey = n.n_nationkey
WHERE c_mktsegment = 'BUILDING'

