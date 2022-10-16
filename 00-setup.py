# Databricks notebook source
# MAGIC %pip install gdown

# COMMAND ----------

import gdown

gdown.download("https://drive.google.com/file/d/16ICDQQkWRdx2D6ixcQ0CTfmXkXiIrPfe/view?usp=sharing", output='/dbfs/tmp/transaction/raw_transaction.csv', fuzzy=True)
gdown.download("https://drive.google.com/file/d/15HPqrOJlZ_NpPyfjTUQagQjQeKIurf04/view?usp=sharing", output='/dbfs/tmp/identity/raw_identity.csv', fuzzy=True)
