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

bronze_path = f"abfss://bronzelayer@ecomdevstg.dfs.core.windows.net/{year}/{month}/25/febmar25m_Inventory.csv"

silver_path = "abfss://silverlayer@ecomdevstg.dfs.core.windows.net/febmar25m_Inventory"

# COMMAND ----------

df_inventory_raw = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(bronze_path)
)


# COMMAND ----------

# 4. Clean, Transform
load_date = current_timestamp()

df_inventory_clean = (
    df_inventory_raw
    .dropDuplicates(["ProductID"])
    .fillna({"StockQuantity": 0})
    .withColumn("StockQuantity", col("StockQuantity").cast("int"))
    .withColumn("CreatedDate", to_timestamp("CreatedDate"))
    .withColumn("ModifiedDate", to_timestamp("ModifiedDate"))

    # Transformation: Low stock flag (e.g., < 10 items)
    .withColumn("IsLowStock", when(col("StockQuantity") < 10, lit(True)).otherwise(lit(False)))

    .withColumn("LoadDate", load_date)
)


# COMMAND ----------

# 5. Write Initial Table (run once only)
#df_inventory_clean.write.format("delta").mode("overwrite").save(silver_path)


# COMMAND ----------

# 6. Merge (SCD Type 1 logic)
delta_inventory = DeltaTable.forPath(spark,silver_path)

delta_inventory.alias("tgt").merge(
    df_inventory_clean.alias("src"),
    "tgt.ProductID = src.ProductID"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()