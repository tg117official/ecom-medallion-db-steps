# ============================================================
# DQ024 - Duplicate catalog listing detection
# Dedup Template
# ============================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder.getOrCreate()

print("=" * 80)
print("PROCESSING bronze_product_catalog_feed")
print("=" * 80)

sample_data = [
    ("CR001","PV101","S001","2026-03-10 09:00:00"),
    ("CR002","PV101","S001","2026-03-10 09:30:00"),
    ("CR003","PV102","S002","2026-03-10 09:40:00")
]
schema = "catalog_record_id string, product_variant_id string, seller_id string, catalog_updated_at string"
df = spark.createDataFrame(sample_data, schema=schema).withColumn("catalog_updated_at", to_timestamp("catalog_updated_at"))

# df = spark.table("ecomsphere.bronze.bronze_product_catalog_feed")

partition_cols = ['product_variant_id', 'seller_id']

order_exprs = [expr(x) for x in ['catalog_updated_at desc', '_ingest_ts desc']]

window_spec = Window.partitionBy(*partition_cols).orderBy(*order_exprs)

ranked_df = df.withColumn("row_num", row_number().over(window_spec))
dedup_df = ranked_df.filter(col("row_num") == 1).drop("row_num")
duplicate_df = ranked_df.filter(col("row_num") > 1).drop("row_num")

print("DEDUPED ROWS")
dedup_df.show(truncate=False)
print("DUPLICATE ROWS")
duplicate_df.show(truncate=False)

# dedup_df.write.format("delta").mode("append").saveAsTable("ecomsphere.silver.silver_dim_product_master")
# duplicate_df.write.format("delta").mode("append").saveAsTable("ecomsphere.silver_quarantine.silver_quarantine.catalog_duplicate_rejections")

