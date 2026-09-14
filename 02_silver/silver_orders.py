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

# COMMAND ----------
# Flag 1: order_placed_at is missing — we can't compute lifecycle durations for this order
orders_clean = orders_clean.withColumn(
    "has_missing_placed_time",
    F.col("order_placed_at").isNull()
)

# COMMAND ----------
# Flag 2: cancelled order that still has a delivered timestamp — contradictory state
orders_clean = orders_clean.withColumn(
    "has_cancelled_but_delivered",
    (F.col("order_status_clean") == "cancelled") & (F.col("order_delivered_at").isNotNull())
)

# COMMAND ----------
# Flag 3: delivered timestamp earlier than ready timestamp — impossible lifecycle order
orders_clean = orders_clean.withColumn(
    "has_illogical_lifecycle_times",
    (F.col("order_delivered_at").isNotNull()) &
    (F.col("order_ready_at").isNotNull()) &
    (F.col("order_delivered_at") < F.col("order_ready_at"))
)

# COMMAND ----------
# Flag 4: order's currency doesn't match its outlet's home currency
orders_clean = orders_clean.join(
    outlets_bronze.select(
        F.col("outlet_id"),
        F.upper(F.trim(F.col("currency"))).alias("outlet_home_currency")
    ),
    on="outlet_id",
    how="left"
)

orders_clean = orders_clean.withColumn(
    "has_currency_mismatch",
    F.upper(F.trim(F.col("currency"))) != F.col("outlet_home_currency")
)

# COMMAND ----------
# Reconciliation: does the order's total_amount match the sum of its line items?
order_items_totals = order_items_bronze.groupBy("order_id").agg(
    F.sum(F.col("quantity") * F.col("unit_price")).alias("computed_items_total")
)

orders_clean = orders_clean.join(order_items_totals, on="order_id", how="left")

orders_clean = orders_clean.withColumn(
    "has_amount_mismatch",
    (F.col("computed_items_total").isNull()) |
    (F.abs(F.col("total_amount") - F.col("computed_items_total")) > 0.01)
)
# COMMAND ----------
orders_clean.select(
    "order_id", "order_type", "order_type_clean",
    "order_status", "order_status_clean",
    "has_missing_placed_time", "has_cancelled_but_delivered",
    "has_illogical_lifecycle_times", "has_currency_mismatch",
    "total_amount", "computed_items_total", "has_amount_mismatch"
).show(truncate=False)