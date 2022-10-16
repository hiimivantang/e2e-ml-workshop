-- Databricks notebook source
-- MAGIC %md
-- MAGIC <img src="https://firebasestorage.googleapis.com/v0/b/firescript-577a2.appspot.com/o/imgs%2Fapp%2Fitang%2FTTSTQ1ZgtA.png?alt=media&token=ae88be18-af93-4db5-891b-5c1f9cb92bcd" alt="drawing" width="1000"/>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <img src="https://firebasestorage.googleapis.com/v0/b/firescript-577a2.appspot.com/o/imgs%2Fapp%2Fitang%2FUrM5msL4a6.png?alt=media&token=192e6e74-2ab6-4368-a6c5-1891bf8efa0e" alt="drawing" width="1000"/>

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE raw_transaction
(CONSTRAINT valid_id EXPECT (TransactionID is not null))
AS SELECT * FROM cloud_files("/tmp/transaction", "csv", map("cloudFiles.inferColumnTypes", "true", "header", "true"))

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE raw_identity
(CONSTRAINT valid_id EXPECT (TransactionID is not null))
AS SELECT * FROM cloud_files("/tmp/identity", "csv", map("cloudFiles.inferColumnTypes", "true", "header", "true"))
