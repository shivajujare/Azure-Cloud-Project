# Databricks notebook source
orders_df = spark.read \
                .format('csv') \
                .option("header",True) \
                .option("inferSchema", True) \
                .option("path","/mnt/retailadls218/raw/orders.csv") \
                .load()
orders_df

# COMMAND ----------

display(orders_df)

# COMMAND ----------

from pyspark.sql.functions import col, count

dups = orders_df.select('order_id') \
                .groupBy('order_id') \
                .agg(count('order_id').alias('dcount')) \
                .filter(col('dcount') > 1) 


# COMMAND ----------

if (dups.rdd.isEmpty() == False):
    dups.write \
        .option("header",True) \
        .format("parquet") \
        .option("path","/mnt/retailadls218/rejects/") \
        .mode("overwrite") \
        .save()
    dbutils.notebook.exit("duplicates found, so exiting the process")



# COMMAND ----------

# MAGIC %run "https://adb-5607707026138194.14.azuredatabricks.net/?o=5607707026138194#notebook/555269385268516"
# MAGIC display(read_df('csv',"/mnt/retailadls218/raw/orders.csv"))

# COMMAND ----------

display(dups.rdd.isEmpty())

# COMMAND ----------


