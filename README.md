# :robot: e2e-ml-workshop :robot:

## 00-setup.py

- [x] Downloading fraud model training datasets from Google drive.
- [x] Original datasets are from https://www.kaggle.com/competitions/ieee-fraud-detection/data

## 01a-fraud detection model building.py 

- [ ] Show high-level solution architecture
- [x] Feature engineering
- [x] Registering features into multiple feature tables
- [x] Model training and experiment auto-tracking with mlflow
- [x] Log model to register feature lookup logic

## 02-feature store deep dive

- [ ] Adding new features to existing feature table
- [ ] Merge/upsert feature tables
- [ ] Publishing features to online store i.e. DynamoDB

## 03-Realtime inference with automatic feature lookup (from online store)
- [ ] Illustrate how serverless model endpoint is integrated with offline/online feature store for automatic feature lookup
- [ ] Create model serving endpoint
- [ ] Score the deployed model via REST API request

## 04-MLOps CI/CD and webhooks

- [ ] Illustrate how CI/CD tooling (e.g. Jenkins) is integrated with model registry
- [ ] Setup webhooks for trigger CI build, i.e. model signature and bias testing

## 05-data and model drift detection

- [ ] Feature, label, prediction, and concept drift detection
- [ ] Triggering retraining of model



# Resources:

Feature store docs
* https://docs.databricks.com/machine-learning/feature-store/feature-tables.html#work-with-feature-tables

Online feature stores
* https://docs.databricks.com/machine-learning/feature-store/online-feature-stores.html


CI Build / model testing and evaluation
* https://docs.databricks.com/mlflow/model-registry-webhooks.html
* http://knowledge-repo-1712701941.us-east-2.elb.amazonaws.com/post/ML/05_ops_validation.kp


Data and model monitoring
* https://www.databricks.com/p/webinar/2021-10-20-hands-on-workshop-unified-ml-monitoring-on-databricks
* https://www.databricks.com/session_na21/drifting_away-testing-ml-models-in-production
* https://www.databricks.com/blog/2019/09/18/productionizing-machine-learning-from-deployment-to-drift-detection.html
