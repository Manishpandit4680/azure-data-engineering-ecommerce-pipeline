# Databricks notebook source
from pyspark.sql.functions import *
from delta.tables import *
from pyspark.sql.types import DoubleType, IntegerType
spark.conf.set(
  "fs.azure.account.key.ecomdevstg.dfs.core.windows.net",
  Access Key
)
# 1. Load Silver Layer Tables
df_products = spark.read.format("delta").load("abfss://silverlayer@ecomdevstg.dfs.core.windows.net/febmar25m_products")
#df_orderitems = spark.read.format("delta").load("/OrderItems").filter("IsCurrent = true")
df_orderitems = (
    spark.read.format("delta").load("abfss://silverlayer@ecomdevstg.dfs.core.windows.net/febmar25m_Orderitems")
    .filter("IsCurrent = true")
    .withColumn("Quantity", col("Quantity").cast(IntegerType()))
    .withColumn("TotalPrice", col("TotalPrice").cast(DoubleType()))
)
df_returns = spark.read.format("delta").load("abfss://silverlayer@ecomdevstg.dfs.core.windows.net/febmar25m_Returns").filter("IsCurrent = true")


# COMMAND ----------

df_sales = (
    df_orderitems
    .groupBy("ProductID")
    .agg(
        sum("Quantity").alias("UnitsSold"),
        sum("TotalPrice").alias("TotalRevenue"),
        round(avg(col("TotalPrice") / col("Quantity")), 2).alias("AvgUnitPrice")
    )
)

# COMMAND ----------

df_product_returns = (
    df_returns
    .groupBy("ProductID")
    .agg(
        count("*").alias("TotalReturns")
    )
)


# COMMAND ----------

df_product_perf = (
    df_products
    .select("ProductID", "ProductName", "Category")
    .join(df_sales, "ProductID", "left")
    .join(df_product_returns, "ProductID", "left")
    .fillna({
        "UnitsSold": 0,
        "TotalRevenue": 0.0,
        "AvgUnitPrice": 0.0,
        "TotalReturns": 0
    })
    .withColumn("ReturnRate", round(col("TotalReturns") / when(col("UnitsSold") > 0, col("UnitsSold")).otherwise(1), 2))
    .withColumn("LoadDate", current_timestamp())
)


# COMMAND ----------

# 4. Write to Gold (ADLS recommended)
gold_path = "abfss://goldlayer@ecomdevstg.dfs.core.windows.net/ProductPerformance"

df_product_perf.write \
    .format("delta") \
    .mode("overwrite") \
    .save(gold_path)