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

bronze_path = f"abfss://bronzelayer@ecomdevstg.dfs.core.windows.net/{year}/{month}/25/febmar25m_Returns.csv"

silver_path = "abfss://silverlayer@ecomdevstg.dfs.core.windows.net/febmar25m_Returns"



# COMMAND ----------

df_returns_raw = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(bronze_path)
)


# COMMAND ----------

# 4. Clean, Transform, Add Derived Columns
load_date = current_timestamp()

df_returns_clean = (
    df_returns_raw
    .dropDuplicates(["ReturnID"])
    .fillna({
        "ReturnReason": "Not Specified",
    })
    .withColumn("ReturnDate", to_timestamp("ReturnDate"))
    .withColumn("CreatedDate", to_timestamp("CreatedDate"))
    .withColumn("ModifiedDate", to_timestamp("ModifiedDate"))

    # Example Transformation: return age in days
    .withColumn("ReturnAgeDays", datediff(current_date(), to_date("ReturnDate")))

    # Add audit & SCD2 columns
    .withColumn("LoadDate", load_date)
    .withColumn("StartDate", load_date)
    .withColumn("EndDate", lit(None).cast("timestamp"))
    .withColumn("IsCurrent", lit(True))
)

# COMMAND ----------

# 5. Write Initial Delta Table (run only once to initialize)
#df_returns_clean.write.format("delta").mode("overwrite").save(silver_path)


# COMMAND ----------

# 6. Merge into Silver Table (SCD Type 2)
delta_returns = DeltaTable.forPath(spark, silver_path)

delta_returns.alias("existing") \
    .merge(
        df_returns_clean.alias("updates"),
        """
        existing.ReturnID = updates.ReturnID AND existing.IsCurrent = true AND (
            existing.ReturnReason != updates.ReturnReason OR
            existing.ProductID != updates.ProductID OR
            existing.OrderID != updates.OrderID
        )
        """
    ) \
    .whenMatchedUpdate(set={
        "EndDate": load_date,
        "IsCurrent": lit(False)
    }) \
    .execute()

delta_returns.alias("existing") \
    .merge(
        df_returns_clean.alias("updates"),
        "existing.ReturnID = updates.ReturnID AND existing.IsCurrent = false"
    ) \
    .whenNotMatchedInsertAll() \
    .execute()