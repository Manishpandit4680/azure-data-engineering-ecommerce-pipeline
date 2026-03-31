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

bronze_path = f"abfss://bronzelayer@ecomdevstg.dfs.core.windows.net/{year}/{month}/25/febmar25m_Customers.csv"

silver_path = "abfss://silverlayer@ecomdevstg.dfs.core.windows.net/febmar25m_Customers"


# COMMAND ----------

print(bronze_path)
print(silver_path)

# COMMAND ----------




# 3. Read CSV from Current Date Path
df_customers_raw = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")    
    .csv(bronze_path)
)

# COMMAND ----------




# 4. Clean & Transform
load_date = current_timestamp()

df_customers_clean = (
    df_customers_raw
    .dropDuplicates(["CustomerID"])
    .withColumn("FullName", concat_ws(" ", col("FirstName"), col("LastName")))
    .withColumn("CreatedDate", to_timestamp("CreatedDate"))
    .withColumn("ModifiedDate", to_timestamp("ModifiedDate"))
    .withColumn("LoadDate", load_date)
    .withColumn("StartDate", load_date)
    .withColumn("EndDate", lit(None).cast("timestamp"))
    .withColumn("IsCurrent", lit(True))
)


# COMMAND ----------


# 5. Write Initial Delta Table (Only once)
# Uncomment and run only once if initializing table
#df_customers_clean.write.format("delta").mode("overwrite").save(silver_path)

# COMMAND ----------



# 6. Load Delta Table

from delta.tables import DeltaTable
from pyspark.sql.functions import *

# Load Delta Table
delta_customers = DeltaTable.forPath(spark, silver_path)

updates_df = df_customers_clean.alias("updates")

# STEP A: Close old records
delta_customers.alias("existing") \
    .merge(
        updates_df,
        "existing.CustomerID = updates.CustomerID AND existing.IsCurrent = true"
    ) \
    .whenMatchedUpdate(
        condition="""
            existing.FirstName <> updates.FirstName OR
            existing.LastName <> updates.LastName OR
            existing.Email <> updates.Email OR
            existing.Phone <> updates.Phone
        """,
        set={
            "EndDate": current_timestamp(),
            "IsCurrent": lit(False)
        }
    ) \
    .execute()

# STEP B: Insert new records
delta_customers.alias("existing") \
    .merge(
        updates_df,
        "existing.CustomerID = updates.CustomerID AND existing.IsCurrent = true"
    ) \
    .whenNotMatchedInsertAll() \
    .execute()

# COMMAND ----------

