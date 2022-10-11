# Deck contents

## Pain Points
- Poor reusability of data and ai assets (i.e. features and models) resulting in 80% of model development time spent on data preparation, feature development, and testing.
- Basic MLOps maturity resulting in 6-8 months for model deployment.
- Lack of data and model validation/monitoring


## Part 1: Introduction to Databricks (30 mins)
- Lakehouse overview
- Delta Lake and it's important role for Machine Learning 
- Solution architecture for realtime fraud detection use case
- Hands-on:

[ ] Creating fully-managed Databricks clusters
[ ] ETL pipeline with automatic data quality validation 

## Part 2: Introduction to Databricks Machine Learning (30 mins)
- Basic overview of Databricks Machine Learning: 
  - Databricks ML runtime, Experiments, Feature Store, Model Registry
- Hands-on: 
[ ] experiment tracking 
[ ] model training
[ ] registering features to feature store


## Part 3: Developing and managing Machine Learning Models (45 mins)
- In-depth model development workflow and CI/CD reference architecture
- Feature store deep dive and best practices
- Manage model lifecycle using MLflow model registry
- Hands-on: 
[ ] Feature Store CRUD
[ ] Synchronizing to online feature store


## Part 4: Deploying and Monitoring Machine Learning Models (45 mins)
- Compare and contrast ML deployment strategies
- Data and concept drift monitoring with Databricks
- Hands-on:
[ ] Deploy a machine learning pipeline in a real-time environment using MLflow model serving, with automatic feature lookup 
[ ] Scoring deployed model via REST API request 

