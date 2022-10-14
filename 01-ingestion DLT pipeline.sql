-- Databricks notebook source
CREATE OR REFRESH STREAMING LIVE TABLE dlt_raw_transaction
AS SELECT * FROM cloud_files("/tmp/transaction", "csv", map("inferSchema", "true", "header", "true"))

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE dlt_raw_identity
AS SELECT * FROM cloud_files("/tmp/identity", "csv", map("inferSchema", "true", "header", "true"))
