# ============================================================
# DQ023 - Duplicate event detection
# Dedup Template
# ============================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder.getOrCreate()

print("=" * 80)
print("PROCESSING bronze_order_event_log")
print("=" * 80)

sample_data = [
    ("E001","O1001","ORDER","2026-03-10 11:00:00"),
    ("E001","O1001","ORDER","2026-03-10 11:05:00"),
    ("E002","O1002","PAYMENT","2026-03-10 11:10:00")
]
schema = "event_id string, order_id string, event_type string, event_timestamp string"
df = spark.createDataFrame(sample_data, schema=schema).withColumn("event_timestamp", to_timestamp("event_timestamp"))

# df = spark.table("ecomsphere.bronze.bronze_order_event_log")

partition_cols = ['event_id']

order_exprs = [expr(x) for x in ['event_timestamp desc', '_ingest_ts desc']]

window_spec = Window.partitionBy(*partition_cols).orderBy(*order_exprs)

ranked_df = df.withColumn("row_num", row_number().over(window_spec))
dedup_df = ranked_df.filter(col("row_num") == 1).drop("row_num")
duplicate_df = ranked_df.filter(col("row_num") > 1).drop("row_num")

print("DEDUPED ROWS")
dedup_df.show(truncate=False)
print("DUPLICATE ROWS")
duplicate_df.show(truncate=False)

# dedup_df.write.format("delta").mode("append").saveAsTable("ecomsphere.silver.silver_order_event_curated")
# duplicate_df.write.format("delta").mode("append").saveAsTable("ecomsphere.silver_quarantine.silver_quarantine.event_duplicate_rejections")

