# Databricks notebook source
# MAGIC %md
# MAGIC Authenticate ABFS using cluster scoped auth
# MAGIC   1) Set spark configs in the cluster itself: separate key-value pairs using a space
# MAGIC   2) List files from demo container
# MAGIC   3) Read data from circuits.csv file

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dladitya.dfs.core.windows.net/"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dladitya.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

