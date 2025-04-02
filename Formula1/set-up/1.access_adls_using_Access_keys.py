# Databricks notebook source
# MAGIC %md
# MAGIC Authenticate ABFS using access keys
# MAGIC   1) Set spark config file
# MAGIC   2) List files from demo container
# MAGIC   3) Read data from circuits.csv file

# COMMAND ----------

formula1_account_key = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1dl-account-key')

# COMMAND ----------

spark.conf.set("fs.azure.account.key.formula1dladitya.dfs.core.windows.net", formula1_account_key)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dladitya.dfs.core.windows.net/"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dladitya.dfs.core.windows.net/circuits.csv"))