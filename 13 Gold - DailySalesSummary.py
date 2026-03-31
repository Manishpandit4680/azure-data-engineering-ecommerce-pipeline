# Databricks notebook source
from pyspark.sql.functions import *
from delta.tables import *
spark.conf.set(
  "fs.azure.account.key.ecomdevstg.dfs.core.windows.net",
  Access Key
)
# 1. Load Silver Tables
df_orders = spark.read.format("delta").load("abfss://silverlayer@ecomdevstg.dfs.core.windows.net/febmar25m_orders").filter("IsCurrent = true")
df_orderitems = spark.read.format("delta").load("abfss://silverlayer@ecomdevstg.dfs.core.windows.net/febmar25m_Orderitems").filter("IsCurrent = true")
df_payments = spark.read.format("delta").load("abfss://silverlayer@ecomdevstg.dfs.core.windows.net/febmar25m_payments")


# COMMAND ----------

df_daily = (
    df_orders.alias("o")
    .join(df_orderitems.alias("oi"), "OrderID", "left")
    .join(df_payments.alias("p"), "OrderID", "left")
)

# COMMAND ----------

# 3. Aggregate by OrderDate
df_daily_summary = (
    df_daily
    .withColumn("OrderDate", to_date("OrderDate"))
    .groupBy("OrderDate")
    .agg(
        countDistinct("OrderID").alias("TotalOrders"),
        sum("oi.Quantity").alias("UnitsSold"),
        sum("oi.TotalPrice").alias("GrossRevenue"),
        sum("p.PaymentAmount").alias("PaymentsCollected"),
        round(avg("oi.TotalPrice"), 2).alias("AvgOrderValue")
    )
    .fillna({
        "UnitsSold": 0,
        "GrossRevenue": 0.0,
        "PaymentsCollected": 0.0,
        "AvgOrderValue": 0.0
    })
    .withColumn("LoadDate", current_timestamp())
)


# COMMAND ----------

# 4. Write to Gold (ADLS recommended)
gold_path = "abfss://goldlayer@ecomdevstg.dfs.core.windows.net/daily_summary"

df_daily_summary.write \
    .format("delta") \
    .mode("overwrite") \
    .save(gold_path)