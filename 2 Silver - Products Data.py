# Databricks notebook source
from pyspark.sql.functions import *
from delta.tables import DeltaTable
import datetime

# 1. Get Current Date
today = datetime.date.today()
year = today.strftime("%Y")
month = today.strftime("%m")
day = today.strftime("%d")

# COMMAND ----------

# 2. Construct Dynamic Bronze Path (based on current date)
spark.conf.set(
  "fs.azure.account.key.ecomdevstg.dfs.core.windows.net",
  Access Key
)

bronze_path = f"abfss://bronzelayer@ecomdevstg.dfs.core.windows.net/{year}/{month}/25/febmar25m_Products.csv"

silver_path = "abfss://silverlayer@ecomdevstg.dfs.core.windows.net/febmar25m_products"

# COMMAND ----------



# COMMAND ----------

df_products_raw = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(bronze_path)
)


# COMMAND ----------

df_products_raw.printSchema()

# COMMAND ----------

load_date = current_timestamp()

df_products_clean = (
    df_products_raw
    .dropDuplicates(["ProductID"])
    .withColumn("Price", col("Price").cast("double"))
    .withColumn("CreatedDate", to_timestamp("CreatedDate"))
    .withColumn("ModifiedDate", to_timestamp("ModifiedDate"))
    .withColumn("LoadDate", load_date)
)

# COMMAND ----------

# 5. Write Initial Table (run only once to initialize)
#df_products_clean.write.format("delta").mode("overwrite").save(silver_path)

# COMMAND ----------



# COMMAND ----------

# 6. SCD Type 1 Merge Logic (overwrite updates, no history)
delta_products = DeltaTable.forPath(spark, silver_path)

delta_products.alias("tgt") \
    .merge(
        df_products_clean.alias("src"),
        "tgt.ProductID = src.ProductID"
    ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()