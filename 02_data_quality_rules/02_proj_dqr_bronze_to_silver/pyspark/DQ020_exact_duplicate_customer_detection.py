# ============================================================
# DQ020 - Exact duplicate customer detection
# Dedup Template
# ============================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder.getOrCreate()

print("=" * 80)
print("PROCESSING bronze_customer")
print("=" * 80)

sample_data = [
    ("C101","Amit","amit@gmail.com","2026-03-10 09:00:00","2026-03-10 09:05:00"),
    ("C101","Amit","amit@gmail.com","2026-03-10 09:00:00","2026-03-10 09:10:00"),
    ("C102","Ravi","ravi@gmail.com","2026-03-10 09:00:00","2026-03-10 09:05:00")
]
schema = "customer_id string, first_name string, email string, created_at string, updated_at string"
df = spark.createDataFrame(sample_data, schema=schema) \
    .withColumn("created_at", to_timestamp("created_at")) \
    .withColumn("updated_at", to_timestamp("updated_at"))

# df = spark.table("ecomsphere.bronze.bronze_customer")

partition_cols = ['customer_id', 'first_name', 'last_name', 'email', 'phone_number', 'status_code']

order_exprs = [expr(x) for x in ['updated_at desc', '_ingest_ts desc']]

window_spec = Window.partitionBy(*partition_cols).orderBy(*order_exprs)

ranked_df = df.withColumn("row_num", row_number().over(window_spec))
dedup_df = ranked_df.filter(col("row_num") == 1).drop("row_num")
duplicate_df = ranked_df.filter(col("row_num") > 1).drop("row_num")

print("DEDUPED ROWS")
dedup_df.show(truncate=False)
print("DUPLICATE ROWS")
duplicate_df.show(truncate=False)

# dedup_df.write.format("delta").mode("append").saveAsTable("ecomsphere.silver.silver_dim_customer_profile")
# duplicate_df.write.format("delta").mode("append").saveAsTable("ecomsphere.silver_quarantine.silver_quarantine.customer_duplicate_rejections")

