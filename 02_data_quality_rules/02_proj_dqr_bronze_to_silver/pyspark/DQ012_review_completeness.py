# ============================================================
# DQ012 - Review completeness
# Completeness Template
# ============================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.getOrCreate()

dq_run_id = "DQ012_RUN_001"

def validate_completeness(df, validation_columns):
    missing_exprs = [when(col(c).isNull(), lit(c)) for c in validation_columns if c in df.columns]
    validated_df = (
        df.withColumn("missing_required_cols", array(*missing_exprs))
          .withColumn("missing_required_cols", expr("filter(missing_required_cols, x -> x is not null)"))
          .withColumn("missing_required_count", size(col("missing_required_cols")))
          .withColumn("dq_null_violation_flag", when(col("missing_required_count") > 0, lit(1)).otherwise(lit(0)))
          .withColumn("null_issue", concat_ws(", ", col("missing_required_cols")))
    )
    return (
        validated_df,
        validated_df.filter(col("dq_null_violation_flag") == 0),
        validated_df.filter(col("dq_null_violation_flag") == 1),
    )

print("=" * 80)
print("PROCESSING bronze_customer_review_feed")
print("=" * 80)

sample_data = [
    ("R001","O1001","C101","P101","PV101",5,"Great","Nice","en","positive","approved",True,10,"media1","2026-03-10 10:00:00","2026-03-10 10:10:00","app",None,"f1.json","2026-03-10 10:20:00","2026-03-10"),
    ("R002","O1002","C102","P102","PV102",4,"Good","Text","en","positive","approved",True,3,"media2",None,"2026-03-10 10:10:00","app",None,"f2.json","2026-03-10 10:20:00","2026-03-10"),
    (None,"O1003",None,"P103","PV103",None,"Bad","Text","en","negative","pending",False,0,"media3","2026-03-10 10:00:00","2026-03-10 10:10:00","app",None,"f3.json","2026-03-10 10:20:00","2026-03-10")
]
schema = """
review_id string, order_id string, customer_id string, product_id string, product_variant_id string, rating int,
review_title string, review_text string, review_language string, review_sentiment string, review_status string, is_verified_purchase boolean,
helpful_votes int, media_urls string, review_created_at string, review_updated_at string, source_system string, _rescued_data string,
_source_file string, _ingest_ts string, bronze_load_date string
"""
df = spark.createDataFrame(sample_data, schema=schema) \
    .withColumn("review_created_at", to_timestamp("review_created_at")) \
    .withColumn("review_updated_at", to_timestamp("review_updated_at")) \
    .withColumn("_ingest_ts", to_timestamp("_ingest_ts")) \
    .withColumn("bronze_load_date", to_date("bronze_load_date"))

# df = spark.table("ecomsphere.bronze.bronze_customer_review_feed")

validation_columns = ['review_id', 'product_id', 'customer_id', 'rating', 'review_created_at']

validated_df, valid_df, invalid_df = validate_completeness(df, validation_columns)

print("VALID ROWS")
valid_df.show(truncate=False)
print("INVALID ROWS")
invalid_df.show(truncate=False)

dq_metrics_df = (
    validated_df.agg(
        count("*").alias("total_records_checked"),
        sum(when(col("dq_null_violation_flag") == 1, 1).otherwise(0)).alias("failed_records_count"),
        sum(when(col("dq_null_violation_flag") == 0, 1).otherwise(0)).alias("passed_records_count")
    )
    .withColumn("dq_run_id", lit(dq_run_id))
    .withColumn("table_name", lit("bronze_customer_review_feed"))
    .withColumn("rule_name", lit("Review completeness"))
    .withColumn("rule_category", lit("Null / Completeness Checks"))
    .withColumn("failure_percentage", round((col("failed_records_count") * 100.0) / col("total_records_checked"), 2))
    .withColumn("run_timestamp", current_timestamp())
    .withColumn("severity", lit("HIGH"))
    .withColumn("status", when(col("failed_records_count") > 0, lit("FAILED")).otherwise(lit("PASSED")))
)

dq_metrics_df.show(truncate=False)

# valid_df.write.format("delta").mode("append").saveAsTable("ecomsphere.silver.silver_customer_review_curated")
# invalid_df.write.format("delta").mode("append").saveAsTable("ecomsphere.silver_quarantine.silver_quarantine.review_completeness_rejections")

