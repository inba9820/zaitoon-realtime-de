# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
from pyspark.sql import functions as F

# COMMAND ----------



# COMMAND ----------
# The old lead_current was built from lead_change_events (status-only,
# 5 columns). Our new source, bronze.lead_operations, has a completely
# different shape (full lead record + operation_type). So we drop the old
# table entirely rather than try to reuse it.
#spark.sql("DROP TABLE IF EXISTS zaitoon_catalog.silver.lead_current")

# COMMAND ----------
# Recreate it empty, matching the new source's real shape this time.
spark.sql("""
    CREATE TABLE IF NOT EXISTS zaitoon_catalog.silver.lead_current
    AS SELECT * FROM zaitoon_catalog.bronze.lead_operations
    WHERE 1=0
""")
spark.table("zaitoon_catalog.silver.lead_current").show()

# COMMAND ----------

# COMMAND ----------
def upsert_to_lead_current(microBatchDF, batchId: int):
    spark_session = microBatchDF.sparkSession
    microBatchDF.createOrReplaceTempView("lead_current_batch")

    spark_session.sql("""
        CREATE OR REPLACE TEMP VIEW lead_current_batch_latest AS
        SELECT * FROM (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY Id
                       ORDER BY event_timestamp DESC
                   ) AS rn
            FROM lead_current_batch
        )
        WHERE rn = 1
    """)

    spark_session.sql("""
        MERGE INTO zaitoon_catalog.silver.lead_current AS target
        USING (SELECT * EXCEPT (rn) FROM lead_current_batch_latest) AS source
        ON target.Id = source.Id
        WHEN MATCHED AND source.operation_type = 'DELETE' THEN DELETE
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED AND source.operation_type != 'DELETE' THEN INSERT *
    """)

query = (
    spark.readStream
        .table("zaitoon_catalog.bronze.lead_operations")
        .writeStream
        .foreachBatch(upsert_to_lead_current)
        .option("checkpointLocation", "/Volumes/zaitoon_catalog/bronze/raw_events/_checkpoints/lead_current_v2")
        .trigger(availableNow=True)
        .start()
)
query.awaitTermination()

# COMMAND ----------

spark.table("zaitoon_catalog.silver.lead_current").count()