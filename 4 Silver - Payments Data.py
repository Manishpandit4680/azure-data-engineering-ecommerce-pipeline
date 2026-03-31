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



# COMMAND ----------

# 2. Construct Dynamic Bronze Path (based on current date)
spark.conf.set(
  "fs.azure.account.key.ecomdevstg.dfs.core.windows.net",
  Access Key
)

bronze_path = f"abfss://bronzelayer@ecomdevstg.dfs.core.windows.net/{year}/{month}/25/febmar25m_Payments.csv"

silver_path = "abfss://silverlayer@ecomdevstg.dfs.core.windows.net/febmar25m_payments"



# COMMAND ----------

# 3. Read from Bronze Layer
df_payments_raw = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(bronze_path)
)

# COMMAND ----------



# COMMAND ----------

# 4. Clean & Transform
load_date = current_timestamp()

df_payments_clean = (
    df_payments_raw
    .dropDuplicates(["PaymentID"])
    .withColumn("PaymentAmount", col("PaymentAmount").cast("double"))
    .withColumn("PaymentDate", to_timestamp("PaymentDate"))
    .withColumn("CreatedDate", to_timestamp("CreatedDate"))
    .withColumn("ModifiedDate", to_timestamp("ModifiedDate"))
    .fillna({
        "PaymentAmount": 0.0
    })
    .withColumn("LoadDate", load_date)
)


# COMMAND ----------

# 5. Write Initial Table (run once)
#df_payments_clean.write.format("delta").mode("overwrite").save(silver_path)

# COMMAND ----------



# COMMAND ----------

# 6. Merge (SCD Type 1 - overwrite changes)
delta_payments = DeltaTable.forPath(spark, silver_path)

delta_payments.alias("tgt").merge(
    df_payments_clean.alias("src"),
    "tgt.PaymentID = src.PaymentID"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()