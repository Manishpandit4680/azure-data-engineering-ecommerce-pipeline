# Databricks notebook source
from pyspark.sql.functions import *
from delta.tables import DeltaTable
import datetime

# 1. Get current date
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

bronze_path = f"abfss://bronzelayer@ecomdevstg.dfs.core.windows.net/{year}/{month}/25/febmar25m_Orderitems.csv"

silver_path = "abfss://silverlayer@ecomdevstg.dfs.core.windows.net/febmar25m_Orderitems"

# COMMAND ----------

df_orderitems_raw = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(bronze_path)
)

# COMMAND ----------

# 4. Clean, Transform, Add Audit Columns
load_date = current_timestamp()

df_orderitems_clean = (
    df_orderitems_raw
    .dropDuplicates(["OrderItemID"])
    .fillna({
        "Quantity": 1,
        "TotalPrice": 0.0
    })
    .withColumn("Quantity", col("Quantity").cast("int"))
    .withColumn("TotalPrice", col("TotalPrice").cast("double"))
    .withColumn("UnitPrice", when(col("Quantity") > 0, col("TotalPrice") / col("Quantity")).otherwise(0.0))
    .withColumn("CreatedDate", to_timestamp("CreatedDate"))
    .withColumn("ModifiedDate", to_timestamp("ModifiedDate"))
    .withColumn("LoadDate", load_date)
    .withColumn("StartDate", load_date)
    .withColumn("EndDate", lit(None).cast("timestamp"))
    .withColumn("IsCurrent", lit(True))
)


# COMMAND ----------

# 5. Write initial Delta table (run only once)
#df_orderitems_clean.write.format("delta").mode("overwrite").save(silver_path)


# COMMAND ----------

# 6. Merge into Silver (SCD Type 2)
delta_orderitems = DeltaTable.forPath(spark, silver_path)

# Expire existing rows
delta_orderitems.alias("tgt") \
    .merge(
        df_orderitems_clean.alias("src"),
        """
        tgt.OrderItemID = src.OrderItemID AND tgt.IsCurrent = true AND (
            tgt.Quantity != src.Quantity OR
            tgt.TotalPrice != src.TotalPrice OR
            tgt.ProductID != src.ProductID OR
            tgt.OrderID != src.OrderID
        )
        """
    ) \
    .whenMatchedUpdate(set={
        "EndDate": load_date,
        "IsCurrent": lit(False)
    }) \
    .execute()

# Insert new records
delta_orderitems.alias("tgt") \
    .merge(
        df_orderitems_clean.alias("src"),
        "tgt.OrderItemID = src.OrderItemID AND tgt.IsCurrent = false"
    ) \
    .whenNotMatchedInsertAll() \
    .execute()