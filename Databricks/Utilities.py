# Databricks notebook source
storage_account = "retailadls218"
client_id            = dbutils.secrets.get(scope="retail-scope", key="sp-client-id")
tenant_id            = dbutils.secrets.get(scope="retail-scope", key="sp-tenant-id")
client_secret        = dbutils.secrets.get(scope="retail-scope", key="sp-secret-value")



# COMMAND ----------



# COMMAND ----------

def mount_adls(container_name, storage_account, client_id, tenant_id, secret):
    configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": f"{client_id}",
           "fs.azure.account.oauth2.client.secret": f"{secret}",
           "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
    
    dbutils.fs.mount(
    source = f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/",
    mount_point = f"/mnt/{storage_account}/{container_name}",
    extra_configs = configs)


# COMMAND ----------

mount_adls('rejects',storage_account,client_id,tenant_id,client_secret )

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

def read_df(vformat, vpath):
    spark.read \
                .format(vformat) \
                .option("header",True) \
                .option("inferSchema", True) \
                .option("path",vpath) \
                .load()

# COMMAND ----------


