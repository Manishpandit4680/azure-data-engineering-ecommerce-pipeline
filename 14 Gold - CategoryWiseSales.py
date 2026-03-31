# Databricks notebook source
from pyspark.sql.functions import *
from delta.tables import *
spark.conf.set(
  "fs.azure.account.key.ecomdevstg.dfs.core.windows.net",
  Access Key
)
# 1. Load Silver Tables
df_products = spark.read.format("delta").load("abfss://silverlayer@ecomdevstg.dfs.core.windows.net/febmar25m_products")
df_orderitems = spark.read.format("delta").load("abfss://silverlayer@ecomdevstg.dfs.core.windows.net/febmar25m_Orderitems").filter("IsCurrent = true")


# COMMAND ----------

# 2. Join OrderItems with Products to get Category info
df_sales_with_category = (
    df_orderitems.alias("oi")
    .join(df_products.select("ProductID", "Category"), "ProductID", "left")
)



# COMMAND ----------

# 3. Cast TotalPrice to numeric type and Aggregate by Category
df_category_summary = (
    df_sales_with_category
    .withColumn("TotalPrice", col("TotalPrice").cast("double"))
    .groupBy("Category")
    .agg(
        sum("Quantity").alias("UnitsSold"),
        sum("TotalPrice").alias("TotalRevenue"),
        round(avg(col("TotalPrice") / col("Quantity")), 2).alias("AvgUnitPrice"),
        countDistinct("ProductID").alias("UniqueProductsSold")
    )
    .fillna({
        "UnitsSold": 0,
        "TotalRevenue": 0.0,
        "AvgUnitPrice": 0.0,
        "UniqueProductsSold": 0
    })
    .withColumn("LoadDate", current_timestamp())
)

# COMMAND ----------

# 4. Write to Gold (ADLS recommended)
gold_path = "abfss://goldlayer@ecomdevstg.dfs.core.windows.net/category_summary"

df_category_summary.write \
    .format("delta") \
    .mode("overwrite") \
    .save(gold_path)