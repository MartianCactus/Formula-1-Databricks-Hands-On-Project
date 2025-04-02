# Databricks notebook source
# MAGIC %md
# MAGIC Authenticate ABFS using Service Principal
# MAGIC

# COMMAND ----------

client_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1dl-app-client-id')
tenant_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-dl-app-tenantid')
client_secret = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-dl-app-clientsecret')


# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1dladitya.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.formula1dladitya.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.formula1dladitya.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.formula1dladitya.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.formula1dladitya.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")


# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dladitya.dfs.core.windows.net/"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dladitya.dfs.core.windows.net/circuits.csv"))