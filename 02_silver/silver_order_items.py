# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------
order_items_bronze = spark.table("zaitoon_catalog.bronze.order_items")
#bronze to silver transformation
# COMMAND ----------
# Flag 1: zero quantity — likely a bad POS entry, not a real transaction
order_items_clean = order_items_bronze.withColumn(
    "has_zero_quantity",
    F.col("quantity") == 0
)

# COMMAND ----------
# Flag 2: negative quantity — likely an unhandled refund/void, not a genuine sale
order_items_clean = order_items_clean.withColumn(
    "has_negative_quantity",
    F.col("quantity") < 0
)

# COMMAND ----------
# Computed line total, treating negative/zero-quantity rows honestly (not hidden)
order_items_clean = order_items_clean.withColumn(
    "line_total",
    F.col("quantity") * F.col("unit_price")
)

# COMMAND ----------
order_items_clean.select(
    "order_id", "item_id", "quantity", "unit_price",
    "has_zero_quantity", "has_negative_quantity", "line_total"
).show(truncate=False)