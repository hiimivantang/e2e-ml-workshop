# Databricks notebook source
import numpy as np                   # array, vector, matrix calculations
import pandas as pd                  # DataFrame handling
import mlflow.pyfunc
import mlflow.spark
from pyspark.sql.functions import col

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Data to featurize

# COMMAND ----------

transactions = table('ieee_cis.raw_transaction')
display(transactions)

# COMMAND ----------

from databricks.feature_store import FeatureStoreClient
fs = FeatureStoreClient()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Offline Feature Store Add / Merge / Update
# MAGIC Use the `write_table` function to update the feature table values.

# COMMAND ----------

# MAGIC %md
# MAGIC When writing, both merge and overwrite modes are supported. To add new features, your input dataframe must have the primary key in a column, and new features in other columns. If the feature already exists, it will not be overwritten by merge.
# MAGIC 
# MAGIC ```
# MAGIC fs.write_table(
# MAGIC   name="streaming_example.streaming_features",
# MAGIC   df=streaming_df,
# MAGIC   mode="merge",
# MAGIC )
# MAGIC ```
# MAGIC 
# MAGIC Features can also be updated or upserted into Feature Store by passing a dataframe with the existing features in columns.
# MAGIC ```
# MAGIC fs.write_table(
# MAGIC   name="feature_store_taxi_example.trip_pickup_features",
# MAGIC   df=pickup_features_df,
# MAGIC   mode="overwrite",
# MAGIC )
# MAGIC ```

# COMMAND ----------

# Adding/Merging new feature (sum of two cards statements) to the existing numerical feature table
additional_features = transactions.withColumn("cardSum", col("card1") + col("card2"))
fs.write_table(df=additional_features[["TransactionID", "cardSum"]], name="ieee_cis.transaction_numerical_features", mode="merge") # Adding and merging uses mode="merge"

# Check if it has been added correctly
display(table("ieee_cis.transaction_numerical_features").select(*["TransactionID","cardSum"]))

# COMMAND ----------

# Updating new feature (sum of two different cards statements) to the existing numerical feature table
additional_features = transactions.withColumn("cardSum", col("card3") + col("card5"))

fs.write_table(df=additional_features[["TransactionID", "cardSum"]], name="ieee_cis.transaction_numerical_features", mode="overwrite") # Upserting uses mode="overwrite"

# Exercice: Check if cardSum has been overwritten. 
# Answer: display(table("ieee_cis.transaction_numerical_features").select(*["TransactionID","cardSum"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Online Feature Store
# MAGIC 
# MAGIC Databricks Feature Store supports publishing features to online feature stores for real-time serving and automated feature lookup. To publish feature tables to an online store, you must provide write authentication to the online store.
# MAGIC 
# MAGIC Databricks recommends that you provide write authentication through an instance profile attached to a Databricks cluster. Alternatively, you can store credentials in Databricks secrets, and then refer to them in a write_secret_prefix when publishing.

# COMMAND ----------

# Getting secrets from Databricks secrets to connect to DynamoDB table where our online feature store will be stored
dbutils.secrets.get('dynamodb','uswest-fs-secret-access-key')

# COMMAND ----------

# Connecting to our online feature store (with DynamoDB)
from databricks.feature_store.online_store_spec import AmazonDynamoDBSpec
online_store = AmazonDynamoDBSpec(region='us-west-1', read_secret_prefix='dynamodb/uswest-fs', write_secret_prefix='dynamodb/uswest-fs') 

# COMMAND ----------

# Connector to the Dynamo DB for queries
import boto3
def get_dynamodb():
  access_key = dbutils.secrets.get('dynamodb','uswest-fs-access-key-id')
  secret_key = dbutils.secrets.get('dynamodb','uswest-fs-secret-access-key')
  region = "us-west-1"
  return boto3.resource('dynamodb',
                 aws_access_key_id=access_key,
                 aws_secret_access_key=secret_key,
                 region_name=region)
  
dynamodb = get_dynamodb()

# COMMAND ----------

# Querying the length of the table where we want to store our online features. This fails when the table has not been created yet. 
table_name = "ieee_cis.transaction_numerical_features"
dynamodb_feature_table = dynamodb.Table(table_name)
print(len(dynamodb_feature_table.scan()['Items']))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Publish batch-computed features to the online store 
# MAGIC You can create and schedule a Databricks job to regularly publish updated features. This job can also include the code to calculate the updated features, or you can create and run separate jobs to calculate and publish feature updates.

# COMMAND ----------

fs.publish_table(
  name=table_name,
  online_store=online_store,
  mode='merge'
)

# COMMAND ----------

# TODO: query more than the dynamodb scan limitation of 1MB
print(len(dynamodb_feature_table.scan()['Items']))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Publish streaming features to the online store
# MAGIC To continuously stream features to the online store, set `streaming=True`.
# MAGIC ```
# MAGIC fs.publish_table(
# MAGIC   name='ieee_cis.transaction_numerical_features',
# MAGIC   online_store=online_store,
# MAGIC   mode='merge',
# MAGIC   streaming=True
# MAGIC )
# MAGIC ```

# COMMAND ----------

# Exercice: Stream the ieee_cis.transaction_numerical_features feature table into the online store and make a change in the offline table to see if it is triggered in the online store.
# Setting the offline feature table to streaming to the online store
fs.publish_table(
  name=table_name,
  online_store=online_store,
  mode='merge',
  streaming=True
)

# Add new feature rows. We generate new entries with existing data. Using describe on TransactionID shows the existing range of ID. We create new IDs in the following code line.
additional_rows = transactions.limit(10).withColumn("TransactionID", col("TransactionID") - 1987000) 
fs.write_table(df=additional_rows, name=table_name, mode="overwrite") # Upserting uses mode="overwrite"

# Checking if the length of the table changed automatically. This number should be different from the previous query.
dynamodb_feature_table = dynamodb.Table(table_name)
len(dynamodb_feature_table.scan()['Items'])
