# Databricks notebook source
def mount_adls(storage_account_name, container_name):

    #Loading Secrets from key vault
    client_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1dl-app-client-id')
    tenant_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-dl-app-tenantid')
    client_secret = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-dl-app-clientsecret')

    #Setting spark configs in dict
    configs = {"fs.azure.account.auth.type": "OAuth",
            "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
            "fs.azure.account.oauth2.client.id": client_id,
            "fs.azure.account.oauth2.client.secret": client_secret,
            "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
    
    #Cheeck if the container is already mounted , and if yes then unmount it before attempting to mount it again
    if any(mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()): 
            dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")
                               
    
    #Mount storage container onto DBFS - according to parameters inputted
    dbutils.fs.mount(
    source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
    mount_point = f"/mnt/{storage_account_name}/{container_name}",
    extra_configs = configs)

    #Display list of mounts
    display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %md
# MAGIC Mount Raw Container

# COMMAND ----------

mount_adls(storage_account_name = 'formula1dladitya', container_name = 'raw')

# COMMAND ----------

# MAGIC %md
# MAGIC Mount processed Container

# COMMAND ----------


mount_adls(storage_account_name = 'formula1dladitya', container_name = 'processed')

# COMMAND ----------

# MAGIC %md
# MAGIC Mount presentation container

# COMMAND ----------


mount_adls(storage_account_name = 'formula1dladitya', container_name = 'presentation')

# COMMAND ----------

mount_adls(storage_account_name = 'formula1dladitya', container_name = 'demo')

# COMMAND ----------

# client_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1dl-app-client-id')
# tenant_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-dl-app-tenantid')
# client_secret = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-dl-app-clientsecret')


# COMMAND ----------

# configs = {"fs.azure.account.auth.type": "OAuth",
#           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
#           "fs.azure.account.oauth2.client.id": client_id,
#           "fs.azure.account.oauth2.client.secret": client_secret,
#           "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}



# COMMAND ----------

# # Optionally, you can add <directory-name> to the source URI of your mount point.
# dbutils.fs.mount(
#   source = "abfss://demo@formula1dladitya.dfs.core.windows.net/",
#   mount_point = "/mnt/formula1dladitya/demo",
#   extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/formula1dladitya/demo"))

# COMMAND ----------

dbutils.fs.ls("/mnt/formula1dladitya/demo")

# COMMAND ----------

# display(spark.read.csv("/mnt/formula1dladitya/demo/circuits.csv"))

# COMMAND ----------

# #Check all the mounts (auto and manually created) in DBFS
display(dbutils.fs.mounts())

# COMMAND ----------


# #Unmount storage
dbutils.fs.unmount('/mnt/formula1dladitya/demo')

# COMMAND ----------

dbutils.fs.refreshMounts()

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt