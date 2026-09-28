# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------
menu_items_bronze = spark.table("zaitoon_catalog.bronze.menu_items")

# COMMAND ----------
menu_items_clean = (
    menu_items_bronze
    .withColumn("item_name", F.trim(F.col("item_name")))
    .withColumn(
        "category_key",
        F.lower(F.trim(F.regexp_replace(F.col("category"), "[-_]", " ")))
    )
    .withColumn(
        "category",
        F.when(F.col("category_key") == "arabian", "Arabian")
         .when(F.col("category_key") == "indo chinese", "Indo-Chinese")
         .when(F.col("category_key") == "north indian", "North Indian")
         .when(F.col("category_key") == "dessert", "Dessert")
         .when(F.col("category_key") == "beverage", "Beverage")
         .otherwise(F.initcap(F.trim(F.col("category"))))
    )
    .withColumn(
        "has_unmapped_category",
        F.coalesce(
            ~F.col("category_key").isin("arabian", "indo chinese", "north indian", "dessert", "beverage"),
            F.lit(True)
        )
    )
    .drop("category_key")
    .withColumn("base_price_inr", F.col("base_price_inr").cast("decimal(10,2)"))
    .withColumn(
        "has_invalid_price",
        F.col("base_price_inr").isNull() | (F.col("base_price_inr") <= 0)
    )
    .withColumn("is_veg", F.col("is_veg").cast("boolean"))
    .select("item_id", "item_name", "category", "base_price_inr", "is_veg",
            "has_unmapped_category", "has_invalid_price")
)

# COMMAND ----------
menu_items_clean.write.format("delta").mode("overwrite").saveAsTable(
    "zaitoon_catalog.silver.menu_items_clean"
)