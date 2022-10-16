# Databricks notebook source
# MAGIC %md
# MAGIC ## Model Registry Webhooks
# MAGIC 
# MAGIC 
# MAGIC ### Supported Events
# MAGIC * Registered model created
# MAGIC * Model version created
# MAGIC * Transition request created
# MAGIC * Model version transitioned stage
# MAGIC 
# MAGIC ### Types of webhooks
# MAGIC * HTTP webhook -- send triggers to endpoints of your choosing such as slack, AWS Lambda, Azure Functions, or GCP Cloud Functions
# MAGIC * Job webhook -- trigger a job within the Databricks workspace
# MAGIC 
# MAGIC ### Use Cases
# MAGIC * Automation - automated introducing a new model to accept shadow traffic, handle deployments and lifecycle when a model is registered, etc..
# MAGIC * Model Artifact Backups - sync artifacts to a destination such as S3 or ADLS
# MAGIC * Automated Pre-checks - perform model tests when a model is registered to reduce long term technical debt
# MAGIC * SLA Tracking - Automatically measure the time from development to production including all the changes inbetween

# COMMAND ----------

import mlflow
from mlflow.utils.rest_utils import http_request
import json

def client():
  return mlflow.tracking.client.MlflowClient()

host_creds = client()._tracking_client.store.get_host_creds()
host = host_creds.host
token = host_creds.token

def mlflow_call_endpoint(endpoint, method, body='{}'):
  if method == 'GET':
      response = http_request(
          host_creds=host_creds, endpoint="/api/2.0/mlflow/{}".format(endpoint), method=method, params=json.loads(body))
  else:
      response = http_request(
          host_creds=host_creds, endpoint="/api/2.0/mlflow/{}".format(endpoint), method=method, json=json.loads(body))
  return response.json()

dbutils.widgets.text("model_name", "model_fraud")
dbutils.widgets.text("staging_job_id", "")
dbutils.widgets.text("production_job_id", "")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Transition Request Created
# MAGIC 
# MAGIC These fire whenever a transition request is created for a model.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Trigger Job

# COMMAND ----------

# TODO, https://docs.databricks.com/applications/mlflow/model-registry-webhooks.html
# Register with the right stage
# TRANSITION_REQUEST_TO_STAGING_CREATED
# TRANSITION_REQUEST_TO_PRODUCTION_CREATED
# MODEL_VERSION_TRANSITIONED_TO_STAGING
# MODEL_VERSION_TRANSITIONED_TO_PRODUCTION

# COMMAND ----------

import json

host = "https://" + dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().get("browserHostName").get()
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()


staging_job_id = dbutils.widgets.get("staging_job_id")

# "events": ["TRANSITION_REQUEST_CREATED"],

trigger_job = json.dumps({
  "model_name": dbutils.widgets.get('model_name'),
  "events": ["TRANSITION_REQUEST_TO_STAGING_CREATED"],
  "description": "Trigger the ops_validation job when a model is moved to staging.",
  "status": "ACTIVE",
  "job_spec": {
    "job_id": staging_job_id,    # This is our 05_staging_validation notebook
    "workspace_url": host,
    "access_token": token
  }
})

mlflow_call_endpoint("registry-webhooks/create", method = "POST", body = trigger_job)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Model Version Transitioned Stage
# MAGIC 
# MAGIC These fire whenever a model successfully transitions to a particular stage.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Trigger Job for production

# COMMAND ----------

# Which model in the registry will we create a webhook for?

#    "events": ["MODEL_VERSION_TRANSITIONED_STAGE"],

production_job_id = dbutils.widgets.get('production_job_id')

trigger_job = json.dumps({
  "model_name": dbutils.widgets.get('model_name'),
  "events": ["MODEL_VERSION_TRANSITIONED_STAGE"],
  "description": "Trigger the ops_validation job when a model is moved to staging to production.",
  "job_spec": {
    "job_id": production_job_id,
    "workspace_url": host,
    "access_token": token
  }
})

mlflow_call_endpoint("registry-webhooks/create", method = "POST", body = trigger_job)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Notifications

# COMMAND ----------

import urllib 
import json 

#     "events": ["MODEL_VERSION_TRANSITIONED_STAGE"],

if slack_webhook:
  trigger_slack = json.dumps({
    "model_name": churn_model_name,
    "events": ["MODEL_VERSION_TRANSITIONED_TO_PRODUCTION"],
    "description": "Notify the MLOps team that a model has moved from Staging to Production.",
    "http_url_spec": {
      "url": slack_webhook
    }
  })
  mlflow_call_endpoint("registry-webhooks/create", method = "POST", body = trigger_slack)
else:
  print("We don't have Slack hook, skipping")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Manage Webhooks

# COMMAND ----------

# MAGIC %md
# MAGIC ##### List 

# COMMAND ----------

list_model_webhooks = json.dumps({"model_name": churn_model_name})

mlflow_call_endpoint("registry-webhooks/list", method = "GET", body = list_model_webhooks)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Delete

# COMMAND ----------

# Remove a webhook
mlflow_call_endpoint("registry-webhooks/delete",
                     method="DELETE",
                     body = json.dumps({'id': 'f814b3d642e74858ad4e61b91ba1ba60'}))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Topics & Resources
# MAGIC 
# MAGIC **Q:** Where can I find out more information on MLflow Model Registry?  
# MAGIC **A:** Check out <a href="https://mlflow.org/docs/latest/registry.html#concepts" target="_blank"> for the latest API docs available for Model Registry</a>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2021 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
