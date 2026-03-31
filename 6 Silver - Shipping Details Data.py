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

bronze_path = f"abfss://bronzelayer@ecomdevstg.dfs.core.windows.net/{year}/{month}/25/febmar25m_ShippingDetails.csv"

silver_path = "abfss://silverlayer@ecomdevstg.dfs.core.windows.net/febmar25m_ShippingDetails"

# COMMAND ----------

# 3. Read CSV from Bronze
df_shipping_raw = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(bronze_path)
)


# COMMAND ----------

load_date = current_timestamp()

df_shipping_clean = (
    df_shipping_raw
    .dropDuplicates(["ShippingID"])
    .fillna({
        "ShippingMethod": "Standard",
        "TrackingNumber": "NA"
    })
    .withColumn("ShipDate", to_timestamp("ShipDate"))
    .withColumn("CreatedDate", to_timestamp("CreatedDate"))
    .withColumn("ModifiedDate", to_timestamp("ModifiedDate"))

    # Transformation: Days since shipped
    .withColumn("ShippingDelayDays", datediff(current_date(), to_date("ShipDate")))

    .withColumn("LoadDate", load_date)
)

# COMMAND ----------

# 5. Write Initial Table (only once)
#df_shipping_clean.write.format("delta").mode("overwrite").save(silver_path)


# COMMAND ----------

# 6. Merge into Silver (SCD Type 1)
delta_shipping = DeltaTable.forPath(spark,silver_path)

delta_shipping.alias("tgt").merge(
    df_shipping_clean.alias("src"),
    "tgt.ShippingID = src.ShippingID"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()