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

bronze_path = f"abfss://bronzelayer@ecomdevstg.dfs.core.windows.net/{year}/{month}/25/febmar25m_Promotions.csv"

silver_path = "abfss://silverlayer@ecomdevstg.dfs.core.windows.net/febmar25m_Promotions"

# COMMAND ----------

df_promotions_raw = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(bronze_path)
)


# COMMAND ----------

# 4. Clean & Transform
load_date = current_timestamp()

df_promotions_clean = (
    df_promotions_raw
    .dropDuplicates(["PromotionID"])
    .fillna({
        "PromotionName": "Unnamed Promo",
        "Discount": 0.0
    })
    .withColumn("Discount", col("Discount").cast("double"))
    .withColumn("StartDate", to_timestamp("StartDate"))
    .withColumn("EndDate", to_timestamp("EndDate"))
    .withColumn("CreatedDate", to_timestamp("CreatedDate"))
    .withColumn("ModifiedDate", to_timestamp("ModifiedDate"))

    # Transformation: flag active promotions
    .withColumn("IsActive", when((current_date() >= to_date("StartDate")) & 
                                 (current_date() <= to_date("EndDate")), lit(True)).otherwise(lit(False)))

    .withColumn("LoadDate", load_date)
)


# COMMAND ----------

# 5. Write Initial Table (only once)
#df_promotions_clean.write.format("delta").mode("overwrite").save(silver_path)


# COMMAND ----------

# 6. Merge to Silver (SCD Type 1)
delta_promotions = DeltaTable.forPath(spark,silver_path)

delta_promotions.alias("tgt").merge(
    df_promotions_clean.alias("src"),
    "tgt.PromotionID = src.PromotionID"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()