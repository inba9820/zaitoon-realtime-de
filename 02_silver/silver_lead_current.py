# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------
# Step 1: create the empty target table, if it doesn't already exist.
# This gives lead_current the exact same columns as lead_change_events,
# but with zero rows — a clean slate for our streaming job to fill in later.
spark.sql("""
    CREATE TABLE IF NOT EXISTS zaitoon_catalog.silver.lead_current
    AS SELECT * FROM zaitoon_catalog.silver.lead_change_events
    WHERE 1=0
""")

# Step 1 check: confirm the table exists with the right columns and no rows
spark.table("zaitoon_catalog.silver.lead_current").show()

# COMMAND ----------
# Step 2: before writing real merge logic, let's just watch how a
# streaming read behaves. This function runs once for every "microbatch"
# of new rows found in lead_change_events — right now it only prints
# them, so we can observe the mechanism before doing anything real with it.
def preview_batch(microBatchDF, batchId: int):
    print(f"--- microbatch {batchId} ---")
    microBatchDF.show()

query = (
    spark.readStream
        .table("zaitoon_catalog.silver.lead_change_events")
        .writeStream
        .foreachBatch(preview_batch)
        .option("checkpointLocation", "/Volumes/zaitoon_catalog/bronze/raw_events/_checkpoints/lead_current_preview")
        .trigger(availableNow=True)
        .start()
)
query.awaitTermination()