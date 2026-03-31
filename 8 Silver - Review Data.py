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

bronze_path = f"abfss://bronzelayer@ecomdevstg.dfs.core.windows.net/{year}/{month}/25/febmar25m_Reviews.csv"

silver_path = "abfss://silverlayer@ecomdevstg.dfs.core.windows.net/febmar25m_Reviews"

# COMMAND ----------

df_reviews_raw = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(bronze_path)
)


# COMMAND ----------

load_date = current_timestamp()

df_reviews_clean = (
    df_reviews_raw
    .dropDuplicates(["ReviewID"])
    .fillna({
        "Comment": "No comment provided",
        "Rating": 0
    })
    .withColumn("Rating", col("Rating").cast("int"))
    .withColumn("Comment", trim(col("Comment")))
    .withColumn("Sentiment",
        when(col("Rating") >= 4, lit("Positive"))
       .when(col("Rating") == 3, lit("Neutral"))
       .otherwise(lit("Negative"))
    )
    .withColumn("CreatedDate", to_timestamp("CreatedDate"))
    .withColumn("ModifiedDate", to_timestamp("ModifiedDate"))
    .withColumn("LoadDate", load_date)
)

# COMMAND ----------

# 5. Write Initial Table (Run Once)
#df_reviews_clean.write.format("delta").mode("overwrite").save(silver_path)


# COMMAND ----------

# 6. Merge Using SCD Type 1
delta_reviews = DeltaTable.forPath(spark,silver_path)

delta_reviews.alias("tgt").merge(
    df_reviews_clean.alias("src"),
    "tgt.ReviewID = src.ReviewID"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()