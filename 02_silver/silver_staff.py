# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------
staff_bronze = spark.table("zaitoon_catalog.bronze.staff")
outlets_clean = spark.table("zaitoon_catalog.silver.outlets_clean")

# COMMAND ----------
staff_clean = (
    staff_bronze
    .withColumn("staff_name", F.trim(F.col("staff_name")))
    .withColumn("role", F.regexp_replace(F.lower(F.trim(F.col("role"))), "[\\s-]+", "_"))
    .withColumn("email", F.lower(F.trim(F.col("email"))))
    .withColumn("has_missing_email", F.col("email").isNull())
    .withColumn(
        "has_invalid_email",
        F.col("email").isNotNull() & ~F.col("email").rlike("^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$")
    )
)

# COMMAND ----------
staff_enriched = (
    staff_clean
    .join(outlets_clean.select("outlet_id", "outlet_name"), on="outlet_id", how="left")
    .withColumn("has_unknown_outlet", F.col("outlet_name").isNull())
    .select("staff_id", "staff_name", "role", "email", "outlet_id", "outlet_name",
            "has_missing_email", "has_invalid_email", "has_unknown_outlet")
)

# COMMAND ----------
staff_enriched.write.format("delta").mode("overwrite").saveAsTable(
    "zaitoon_catalog.silver.staff_clean"
)