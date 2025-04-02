# Databricks notebook source
# MAGIC %md
# MAGIC 1. Get client_is, tenant_id and client_Secret
# MAGIC 2. Set spark config with app/clientid, directory/tenantid & secret
# MAGIC 3. Call file system utility mount to mount the storage
# MAGIC 4. Explore other file system utilities related to mount (list all mounts, unmount)
# MAGIC

# COMMAND ----------

client_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1dl-app-client-id')
tenant_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-dl-app-tenantid')
client_secret = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-dl-app-clientsecret')


# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}



# COMMAND ----------

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://demo@formula1dladitya.dfs.core.windows.net/",
  mount_point = "/mnt/formula1dladitya/demo",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/formula1dladitya/demo"))

# COMMAND ----------

display(spark.read.csv("/mnt/formula1dladitya/demo/circuits.csv"))

# COMMAND ----------

#Check all the mounts (auto and manually created) in DBFS
display(dbutils.fs.mounts())

# COMMAND ----------


#Unmount storage
dbutils.fs.unmount('/mnt/formula1dladitya/demo')