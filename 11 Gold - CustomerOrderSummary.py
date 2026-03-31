# Databricks notebook source
from pyspark.sql.functions import *
from delta.tables import *
spark.conf.set(
  "fs.azure.account.key.ecomdevstg.dfs.core.windows.net",
  Access Key
)
df_customers = spark.read.format("delta").load("abfss://silverlayer@ecomdevstg.dfs.core.windows.net/febmar25m_Customers")

df_orders = spark.read.format("delta").load("abfss://silverlayer@ecomdevstg.dfs.core.windows.net/febmar25m_orders").filter("IsCurrent = true")

df_orderitems = spark.read.format("delta").load("abfss://silverlayer@ecomdevstg.dfs.core.windows.net/febmar25m_Orderitems").filter("IsCurrent = true")

df_payments = spark.read.format("delta").load("abfss://silverlayer@ecomdevstg.dfs.core.windows.net/febmar25m_payments")

# COMMAND ----------

# 2. Join Orders → OrderItems → Payments
df_orders_joined = (
    df_orders.alias("o")
    .join(df_orderitems.alias("oi"), col("o.OrderID") == col("oi.OrderID"), "left")
    .join(df_payments.alias("p"), col("o.OrderID") == col("p.OrderID"), "left")
)

# COMMAND ----------

# 3. Aggregate at Customer level
df_customer_summary = (
    df_orders_joined
    .groupBy("o.CustomerID")
    .agg(
        countDistinct("o.OrderID").alias("TotalOrders"),
        sum("oi.TotalPrice").alias("TotalSpent"),
        round(avg("oi.TotalPrice"), 2).alias("AvgOrderValue"),
        min("o.OrderDate").alias("FirstOrderDate"),
        max("o.OrderDate").alias("RecentOrderDate")
    )
)


# COMMAND ----------

# 4. Join with Customer Details
df_customer_summary_final = (
    df_customers.select("CustomerID", "FirstName", "LastName", "Email")
    .join(df_customer_summary, "CustomerID", "left")
    .fillna({
        "TotalOrders": 0,
        "TotalSpent": 0.0,
        "AvgOrderValue": 0.0
    })
    .withColumn("LoadDate", current_timestamp())
)


# COMMAND ----------

# 4. Write to Gold (ADLS recommended)
gold_path = "abfss://goldlayer@ecomdevstg.dfs.core.windows.net/customer_summary_final"

df_customer_summary_final.write \
    .format("delta") \
    .mode("overwrite") \
    .save(gold_path)

# COMMAND ----------

display(df_customer_summary_final)