# Databricks notebook source
# MAGIC %md
# MAGIC ### Use the logged model for inference 
# MAGIC We use the model we have just trained on the Feature Store. By passing it only the TransactionID, it will automatically fetch other features in the Feature Store and predict the probability that the transaction ID is a fraud.

# COMMAND ----------

import pandas as pd
from pyspark.sql.types import StructType, StructField, IntegerType
from databricks.feature_store import FeatureStoreClient
fs = FeatureStoreClient()

# We create a DataFrame with an existing transactionID we want to test (2995503)
schema = StructType([StructField("TransactionID", IntegerType(), True)])

inference_df = spark.createDataFrame(pd.DataFrame(columns=["TransactionID"], data=[2995503]), schema=schema)

predictions = fs.score_batch(
  'models:/model_fraud/2',
  df=inference_df
)
