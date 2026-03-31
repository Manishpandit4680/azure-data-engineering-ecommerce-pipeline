# Databricks notebook source
from pyspark.sql.functions import *
from delta.tables import DeltaTable
import datetime

# COMMAND ----------

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

bronze_path = f"abfss://bronzelayer@ecomdevstg.dfs.core.windows.net/{year}/{month}/25/febmar25m_Orders.csv"

silver_path = "abfss://silverlayer@ecomdevstg.dfs.core.windows.net/febmar25m_orders"



# COMMAND ----------

df_orders_raw = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(bronze_path)
)


# COMMAND ----------

# 4. Clean & Transform
load_date = current_timestamp()

df_orders_clean = (
    df_orders_raw
    # Remove duplicates
    .dropDuplicates(["OrderID"])
    
    # Convert to correct types
    .withColumn("OrderDate", to_timestamp("OrderDate"))
    .withColumn("CreatedDate", to_timestamp("CreatedDate"))
    .withColumn("ModifiedDate", to_timestamp("ModifiedDate"))

    # Handle nulls and invalid values
    .fillna({
        "OrderStatus": "Unknown"
    })

    # Add audit & SCD2 columns
    .withColumn("LoadDate", load_date)
    .withColumn("StartDate", load_date)
    .withColumn("EndDate", lit(None).cast("timestamp"))
    .withColumn("IsCurrent", lit(True))
)

# COMMAND ----------

display(df_orders_clean)

# COMMAND ----------



# COMMAND ----------

# 5. Write Initial Table (Run only once to initialize)
#df_orders_clean.write.format("delta").mode("overwrite").save(silver_path)

# COMMAND ----------



# COMMAND ----------

# 6. SCD Type 2 Merge Logic
delta_orders = DeltaTable.forPath(spark, silver_path)
updates_df = df_orders_clean.alias("updates")

# A. Mark previous records as not current if data has changed
delta_orders.alias("existing") \
    .merge(
        updates_df,
        """
        existing.OrderID = updates.OrderID AND existing.IsCurrent = true AND (
            existing.OrderStatus != updates.OrderStatus
        )
        """
    ) \
    .whenMatchedUpdate(set={
        "EndDate": load_date,
        "IsCurrent": lit(False)
    }) \
    .execute()

# B. Insert new version of changed or new records
delta_orders.alias("existing") \
    .merge(
        updates_df,
        "existing.OrderID = updates.OrderID AND existing.IsCurrent = false"
    ) \
    .whenNotMatchedInsertAll() \
    .execute()

# COMMAND ----------

display(df_orders_clean)