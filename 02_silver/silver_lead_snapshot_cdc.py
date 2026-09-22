# Databricks notebook source
from pyspark.sql import functions as F

# # COMMAND ----------
# # Take a full copy of the current lead table, and stamp it with the exact time we took it
# lead_snapshot = spark.table("zaitoon_catalog.bronze.lead").withColumn(
#     "snapshot_taken_at", F.current_timestamp()
# )

# lead_snapshot.write.format("delta").mode("append").saveAsTable(
#     "zaitoon_catalog.silver.lead_snapshots"
# )

# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------
snapshot_times = [
    row.snapshot_taken_at for row in
    spark.table("zaitoon_catalog.silver.lead_snapshots")
    .select("snapshot_taken_at").distinct().orderBy("snapshot_taken_at").collect()
]
old_snapshot_time = snapshot_times[-2]
new_snapshot_time = snapshot_times[-1]

# COMMAND ----------
compare_cols = ["FirstName", "LastName", "Company", "Email", "Phone", "Status"]

old_snapshot = spark.table("zaitoon_catalog.silver.lead_snapshots").filter(
    F.col("snapshot_taken_at") == old_snapshot_time
)
new_snapshot = spark.table("zaitoon_catalog.silver.lead_snapshots").filter(
    F.col("snapshot_taken_at") == new_snapshot_time
)

old_hashed = old_snapshot.withColumn(
    "row_hash", F.sha2(F.concat_ws("||", *compare_cols), 256)
).select(F.col("Id"), F.col("row_hash").alias("old_hash"), F.col("Status").alias("old_status"))

new_hashed = new_snapshot.withColumn(
    "row_hash", F.sha2(F.concat_ws("||", *compare_cols), 256)
).select(F.col("Id"), F.col("row_hash").alias("new_hash"), F.col("Status").alias("new_status"))

# COMMAND ----------
lead_diff = old_hashed.join(new_hashed, on="Id", how="full_outer")

lead_diff = lead_diff.withColumn(
    "change_type",
    F.when(F.col("old_hash").isNull(), "INSERT")
     .when(F.col("new_hash").isNull(), "DELETE")
     .when(F.col("old_hash") != F.col("new_hash"), "UPDATE")
     .otherwise("UNCHANGED")
)

# COMMAND ----------
lead_change_events = lead_diff.filter(
    F.col("change_type") != "UNCHANGED"
).withColumn(
    "detected_at", F.current_timestamp()
).select(
    "Id", "change_type", "old_status", "new_status", "detected_at"
)

lead_change_events.write.format("delta").mode("append").saveAsTable(
    "zaitoon_catalog.silver.lead_change_events"
)


