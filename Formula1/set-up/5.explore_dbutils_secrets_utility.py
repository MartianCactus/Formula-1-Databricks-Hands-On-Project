# Databricks notebook source
# MAGIC %md
# MAGIC ###Explore the capabilities of the secrets utility

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list("formula1-scope")

# COMMAND ----------

dbutils.secrets.get(scope = 'formula1-scope', key='formula1dl-account-key') 