# Databricks notebook source
# MAGIC %run ./00-setup

# COMMAND ----------

import numpy as np                   # array, vector, matrix calculations
import pandas as pd                  # DataFrame handling
import xgboost as xgb                # gradient boosting machines (GBMs)
import mlflow
import os
import mlflow.pyfunc
import mlflow.spark

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Featurization
# MAGIC 
# MAGIC ![](https://firebasestorage.googleapis.com/v0/b/firescript-577a2.appspot.com/o/imgs%2Fapp%2FCBT-JOURNAL%2FPGXlTpZ9aL.png?alt=media&token=9feaa0a6-3fe7-4ddf-ae9b-b62979bf9887)

# COMMAND ----------

transactions = table('ieee_cis.raw_transaction')
display(transactions)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC **Categorical features**
# MAGIC 
# MAGIC * ProductCD (string)
# MAGIC * card1 - card6
# MAGIC * addr1, addr2
# MAGIC * P_emaildomain (string)
# MAGIC * R_emaildomain (string)
# MAGIC * M1 - M9 (string)

# COMMAND ----------

stringCategoricalCols = ['ProductCD',
                         'P_emaildomain',
                         'R_emaildomain',
                         'card4',
                         'card6',
                         'M1',
                         'M2',
                         'M3',
                         'M4',
                         'M5',
                         'M6',
                         'M7',
                         'M8',
                         'M9']

categoricalCols = ['card1',
                   'card2',
                   'card3',
                   'card5',
                   'addr1',
                   'addr2']

numericalCols = ['dist1','dist2']+['C' + str(x) for x in range(1,14)]+['D' + str(x) for x in range(1,15)]+['V' + str(x) for x in range(1,339)]

# COMMAND ----------

import pyspark.pandas as ps
import numpy as np

# COMMAND ----------

numerical_features = transactions.select(*(['TransactionID']+numericalCols))
numerical_features = numerical_features.toDF(*(c.replace('.', '_') for c in numerical_features.columns))
numerical_features = numerical_features.toDF(*(c.replace(' ', '_') for c in numerical_features.columns))

categorical_features = ps.get_dummies(transactions.select(['TransactionID']+stringCategoricalCols+categoricalCols).pandas_api(), columns=stringCategoricalCols+categoricalCols, dummy_na=True, dtype=np.int32).to_spark()
categorical_features = categorical_features.toDF(*(c.replace('.', '_') for c in categorical_features.columns))
categorical_features = categorical_features.toDF(*(c.replace(' ', '_') for c in categorical_features.columns))

# COMMAND ----------

from databricks.feature_store import FeatureStoreClient

fs = FeatureStoreClient()
try:
  categorical_feature_table = fs.create_table(
    name='ieee_cis.transaction_categorical_features',
    primary_keys='TransactionID',
    schema=categorical_features.schema,
    description='These features are derived from ieee_cis.raw_transactions and the label column (isFraud) has being dropped'
  )
except ValueError as v:
  if "already exists with a different schema" in str(v):
    pass
  else:
    raise
except Exception as  e:
  raise

spark.sql("ALTER TABLE ieee_cis.transaction_categorical_features SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name','delta.minReaderVersion' = '2','delta.minWriterVersion' = '5')")

fs.write_table(df=categorical_features, name='ieee_cis.transaction_categorical_features', mode='overwrite')

# COMMAND ----------

try:
  numerical_feature_table = fs.create_table(
    name='ieee_cis.transaction_numerical_features',
    primary_keys='TransactionID',
    schema=numerical_features.schema,
    description='These features are derived from ieee_cis.raw_transactions and the label column (isFraud) has being dropped'
  )
except ValueError as v:
  if "already exists with a different schema" in str(v):
    pass
  else:
    raise
except Exception as  e:
  raise

spark.sql("ALTER TABLE ieee_cis.transaction_numerical_features SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name','delta.minReaderVersion' = '2','delta.minWriterVersion' = '5')")
fs.write_table(df=numerical_features, name='ieee_cis.transaction_numerical_features', mode='overwrite')

# COMMAND ----------

from databricks.feature_store import FeatureLookup

feature_lookups = [
  FeatureLookup(
    table_name = 'ieee_cis.transaction_numerical_features',
    lookup_key = ['TransactionID']
  ),
  FeatureLookup(
    table_name = 'ieee_cis.transaction_categorical_features',
    lookup_key = ['TransactionID']
  )
]

# COMMAND ----------

def fit(X, y):
  """
   :return: dict with fields 'loss' (scalar loss) and 'model' fitted model instance
  """
  import xgboost
  from xgboost import XGBRegressor
  from sklearn.model_selection import cross_val_score
  
  _model =  XGBRegressor(learning_rate=0.3,
                          gamma=5,
                          max_depth=8,
                          n_estimators=15,
                          min_child_weight = 9, objective='reg:squarederror')
 
  xgb_model = _model.fit(X, y)
  
  score = -cross_val_score(_model, X, y, scoring='neg_mean_squared_error').mean()
  
  return {'loss': score, 'model': xgb_model}

# COMMAND ----------

from databricks.feature_store import FeatureStoreClient
import mlflow

fs = FeatureStoreClient()
mlflow.xgboost.autolog()

with mlflow.start_run():
  training_set = fs.create_training_set(
  df = table('ieee_cis.raw_transaction').select(*['TransactionID','isFraud']),
  feature_lookups = feature_lookups,
  label = 'isFraud')
  
  training_df = training_set.load_df().toPandas()
  
  X_train = training_df.drop(['isFraud'], axis=1)
  y_train = training_df.isFraud
  train_dict = fit(X=X_train, y=y_train)
  xgb_model = train_dict['model']
  mlflow.log_metric('loss', train_dict['loss'])

# COMMAND ----------


