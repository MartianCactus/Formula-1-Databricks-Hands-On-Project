# Databricks notebook source
# MAGIC %md
# MAGIC Authenticate ABFS using SAS token
# MAGIC   1) Set spark config file
# MAGIC   2) List files from demo container
# MAGIC   3) Read data from circuits.csv file

# COMMAND ----------

# MAGIC %pip install azure-storage

# COMMAND ----------

formula1_sas_token = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1dl-demo-sas-token-2')

# COMMAND ----------

#Setting authentication type as SAS
spark.conf.set("fs.azure.account.auth.type.formula1dladitya.dfs.core.windows.net", "SAS")

#Setting token type as fixed SAS
spark.conf.set("fs.azure.sas.token.provider.type.formula1dladitya.dfs.core.windows.net", "org.apache.hadoop.fs.azurevfs.sas.FixedSASTokenProvider")

#Provice SAS token value
spark.conf.set("fs.azure.sas.fixed.token.formula1dladitya.dfs.core.windows.net", formula1_sas_token)


# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dladitya.dfs.core.windows.net/"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dladitya.dfs.core.windows.net/circuits.csv"))