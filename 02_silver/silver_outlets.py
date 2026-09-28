# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------
outlets_bronze = spark.table("zaitoon_catalog.bronze.outlets")

# COMMAND ----------
outlets_clean = (
    outlets_bronze
    .withColumn("outlet_name", F.trim(F.col("outlet_name")))
    .withColumn("city", F.initcap(F.trim(F.col("city"))))
    .withColumn("city", F.when(F.col("city") == "Bangalore", "Bengaluru")
                         .otherwise(F.col("city")))
    .withColumn("country_key", F.upper(F.trim(F.col("country"))))
    .withColumn("country", F.when(F.col("country_key") == "INDIA", "India")
                            .when(F.col("country_key") == "UAE", "UAE")
                            .when(F.col("country_key") == "QATAR", "Qatar")
                            .otherwise(F.col("country")))
    .drop("country_key")
    .withColumn("currency", F.upper(F.trim(F.col("currency"))))
    .withColumn("outlet_type", F.lower(F.trim(F.col("outlet_type"))))
    .withColumn("opened_date", F.to_date(F.col("opened_date")))
    .withColumn("has_missing_opened_date", F.col("opened_date").isNull())
    .select("outlet_id", "outlet_name", "city", "country", "currency",
            "outlet_type", "opened_date", "has_missing_opened_date")
)

# COMMAND ----------
outlets_clean.write.format("delta").mode("overwrite").saveAsTable(
    "zaitoon_catalog.silver.outlets_clean"
)