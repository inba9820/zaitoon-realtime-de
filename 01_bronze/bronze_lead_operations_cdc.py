# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------
# ONE-TIME SETUP — run this cell only once, ever. Creates the new empty
# operations log, with the same columns as bronze.lead plus our three
# tracking columns. We are NOT renaming bronze.lead — that table is owned
# by the Lakeflow Connect Salesforce pipeline, and renaming it would break
# that pipeline's next run. We just read from it as-is, treating it as our
# staging input by convention only, not by name.
# spark.sql("""
#     CREATE TABLE zaitoon_catalog.bronze.lead_operations AS
#     SELECT
#         *,
#         CAST(NULL AS STRING)    AS operation_type,
#         CAST(NULL AS TIMESTAMP) AS event_timestamp,
#         CAST(NULL AS STRING)    AS row_hash
#     FROM zaitoon_catalog.bronze.lead
#     WHERE 1=0
# """)

# # COMMAND ----------
# # Confirm it worked
# spark.sql("SHOW TABLES IN zaitoon_catalog.bronze").show()
# spark.table("zaitoon_catalog.bronze.lead_operations").printSchema()

# COMMAND ----------
# Columns Salesforce updates automatically just from being viewed or synced —
# not genuine business changes. We still store their values in the row, but
# they're excluded from the hash so they don't trigger false UPDATE events.
volatile_cols = ["LastModifiedDate", "SystemModstamp", "LastViewedDate", "LastReferencedDate", "IsUnreadByOwner"]

staging = spark.table("zaitoon_catalog.bronze.lead")
hash_cols = [c for c in staging.columns if c not in volatile_cols]

staging_hashed = staging.withColumn(
    "row_hash",
    F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in hash_cols]), 256)
)

# COMMAND ----------
# For each lead, find the most recent operation already logged for it (if any)
from pyspark.sql.window import Window

ops_table = "zaitoon_catalog.bronze.lead_operations"
latest_per_lead = Window.partitionBy("Id").orderBy(F.col("event_timestamp").desc())

latest_ops = (
    spark.table(ops_table)
    .withColumn("rn", F.row_number().over(latest_per_lead))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

# COMMAND ----------
# Compare current staging hash vs each lead's latest logged hash
diff = (
    staging_hashed.select("Id", F.col("row_hash").alias("new_hash"))
    .join(latest_ops.select("Id", F.col("row_hash").alias("old_hash")), on="Id", how="full_outer")
    .withColumn(
        "operation_type",
        F.when(F.col("old_hash").isNull(), "CREATE")
         .when(F.col("new_hash").isNull(), "DELETE")
         .when(F.col("old_hash") != F.col("new_hash"), "UPDATE")
         .otherwise("UNCHANGED")
    )
)

changed = diff.filter(F.col("operation_type") != "UNCHANGED").select("Id", "operation_type")

# COMMAND ----------
# CREATE / UPDATE: take the full current row from staging
create_and_update = staging_hashed.join(
    changed.filter(F.col("operation_type").isin("CREATE", "UPDATE")), on="Id"
).withColumn("event_timestamp", F.current_timestamp())

# DELETE: lead is gone from staging, so carry forward its last known full row
deletes = (
    latest_ops.join(changed.filter(F.col("operation_type") == "DELETE").select("Id"), on="Id")
    .drop("operation_type", "event_timestamp")
    .withColumn("operation_type", F.lit("DELETE"))
    .withColumn("event_timestamp", F.current_timestamp())
)

# COMMAND ----------
new_events = create_and_update.unionByName(deletes)
new_events.write.format("delta").mode("append").saveAsTable(ops_table)

