# ============================================================
# DQ022 - Duplicate review detection
# Dedup Template
# ============================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder.getOrCreate()

print("=" * 80)
print("PROCESSING bronze_customer_review_feed")
print("=" * 80)

sample_data = [
    ("R001","C101","PV101","2026-03-10 10:00:00","2026-03-10 10:10:00"),
    ("R001","C101","PV101","2026-03-10 10:00:00","2026-03-10 10:20:00"),
    ("R002","C102","PV102","2026-03-10 10:00:00","2026-03-10 10:10:00")
]
schema = "review_id string, customer_id string, product_variant_id string, review_created_at string, review_updated_at string"
df = spark.createDataFrame(sample_data, schema=schema) \
    .withColumn("review_created_at", to_timestamp("review_created_at")) \
    .withColumn("review_updated_at", to_timestamp("review_updated_at"))

# df = spark.table("ecomsphere.bronze.bronze_customer_review_feed")

partition_cols = ['review_id']

order_exprs = [expr(x) for x in ['review_updated_at desc', '_ingest_ts desc']]

window_spec = Window.partitionBy(*partition_cols).orderBy(*order_exprs)

ranked_df = df.withColumn("row_num", row_number().over(window_spec))
dedup_df = ranked_df.filter(col("row_num") == 1).drop("row_num")
duplicate_df = ranked_df.filter(col("row_num") > 1).drop("row_num")

print("DEDUPED ROWS")
dedup_df.show(truncate=False)
print("DUPLICATE ROWS")
duplicate_df.show(truncate=False)

# dedup_df.write.format("delta").mode("append").saveAsTable("ecomsphere.silver.silver_customer_review_curated")
# duplicate_df.write.format("delta").mode("append").saveAsTable("ecomsphere.silver_quarantine.silver_quarantine.review_duplicate_rejections")

