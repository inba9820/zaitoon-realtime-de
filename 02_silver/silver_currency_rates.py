# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------
rates_bronze = spark.table("zaitoon_catalog.bronze.currency_conversion_rates")

# COMMAND ----------
rates_clean = (
    rates_bronze
    .withColumn("currency_code", F.upper(F.trim(F.col("currency_code"))))
    .withColumn("rate_to_inr", F.col("rate_to_inr").cast("decimal(10,4)"))
    .withColumn("effective_date", F.to_date(F.col("effective_date")))
    .withColumn(
        "has_invalid_rate",
        F.col("rate_to_inr").isNull() | (F.col("rate_to_inr") <= 0)
    )
)

# COMMAND ----------
rate_window = Window.partitionBy("currency_code").orderBy("effective_date")

rates_versioned = (
    rates_clean
    .withColumn("valid_from", F.col("effective_date"))
    .withColumn("valid_to", F.lead("effective_date").over(rate_window))
    .withColumn("is_current", F.col("valid_to").isNull())
    .select("currency_code", "rate_to_inr", "valid_from", "valid_to",
            "is_current", "has_invalid_rate")
)

# COMMAND ----------
rates_versioned.write.format("delta").mode("overwrite").saveAsTable(
    "zaitoon_catalog.silver.currency_conversion_rates_clean"
)