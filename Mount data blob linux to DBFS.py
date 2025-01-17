# Databricks notebook source
# Mount ADLS Gen2 storage to Databricks File System (DBFS)
configs = {
  "fs.azure.account.auth.type": "OAuth",
  "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
  "fs.azure.account.oauth2.client.id": "352f8427-ab0f-4fea-bcab-766a66b18860",
  "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="mysecret", key="mysecretskey"),
  "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/f6cb831a-7ff7-4563-a35b-6d6ca3b3f036/oauth2/token"
}

# Mount the ADLS container (replace <container-name> with your container name)
dbutils.fs.mount(
  source = "abfss://lkmpro@sahebstoragelake.dfs.core.windows.net/",
  mount_point = "/mnt/lkmpro",
  extra_configs = configs
)

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

dbutils.fs.refreshMounts()
