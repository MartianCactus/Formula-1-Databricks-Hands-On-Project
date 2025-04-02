# Databricks notebook source
# DBTITLE 1,er
# MAGIC %md
# MAGIC
# MAGIC 1) dbutils.fs.ls('/')
# MAGIC 2) Interact DBFS file browser
# MAGIC 3) Upload file to DBFS root

# COMMAND ----------

display(dbutils.fs.ls('/'))

# COMMAND ----------

display(dbutils.fs.ls('/FileStore'))

# COMMAND ----------

display(spark.read.csv('/FileStore/tables/'))