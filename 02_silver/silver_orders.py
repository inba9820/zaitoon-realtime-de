# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------
orders_bronze = spark.table("zaitoon_catalog.bronze.orders")
order_items_bronze = spark.table("zaitoon_catalog.bronze.order_items")
outlets_bronze = spark.table("zaitoon_catalog.bronze.outlets")

# COMMAND ----------
# Standardize order_type: trim whitespace, lowercase, replace underscores with spaces,
# then map every known variant to one canonical value
orders_clean = orders_bronze.withColumn(
    "order_type_clean",
    F.lower(F.trim(F.regexp_replace(F.col("order_type"), "[_-]", " ")))
)
orders_clean = orders_clean.withColumn(
    "order_type_clean",
    F.when(F.col("order_type_clean").isin("dine in", "dinein"), "dine-in")
     .otherwise(F.col("order_type_clean"))
)

# Standardize order_status the same way (just trim + lowercase, no separators to fix here)
orders_clean = orders_clean.withColumn(
    "order_status_clean",
    F.lower(F.trim(F.col("order_status")))
)