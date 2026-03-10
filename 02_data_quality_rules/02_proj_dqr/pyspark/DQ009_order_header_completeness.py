# ============================================================
# DQ009 - Order header completeness
# Completeness Template
# ============================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.getOrCreate()

dq_run_id = "DQ009_RUN_001"

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
print("PROCESSING bronze_orders")
print("=" * 80)

sample_data = [
    ("O1001","C101","2026-03-10 10:00:00","PLACED","APP","INR","A1","A2",100,10,5,20,115,"2026-03-10 10:01:00","2026-03-10 10:02:00",False,"2026-03-10 10:03:00","2026-03-10"),
    ("O1002",None,"2026-03-10 10:00:00","PLACED","APP","INR","A1","A2",100,10,5,20,115,"2026-03-10 10:01:00","2026-03-10 10:02:00",False,"2026-03-10 10:03:00","2026-03-10"),
    (None,"C103",None,None,"APP","INR","A1","A2",100,10,5,20,115,"2026-03-10 10:01:00","2026-03-10 10:02:00",False,"2026-03-10 10:03:00","2026-03-10")
]
schema = """
order_id string, customer_id string, order_date string, order_status_code string, order_channel_code string, currency_code string,
billing_address_id string, shipping_address_id string, total_item_amount double, total_discount_amount double, total_tax_amount double,
total_shipping_amount double, grand_total_amount double, created_at string, updated_at string, is_deleted boolean, _ingest_ts string, bronze_load_date string
"""
df = spark.createDataFrame(sample_data, schema=schema) \
    .withColumn("order_date", to_timestamp("order_date")) \
    .withColumn("created_at", to_timestamp("created_at")) \
    .withColumn("updated_at", to_timestamp("updated_at")) \
    .withColumn("_ingest_ts", to_timestamp("_ingest_ts")) \
    .withColumn("bronze_load_date", to_date("bronze_load_date"))

# df = spark.table("ecomsphere.bronze.bronze_orders")

validation_columns = ['order_id', 'customer_id', 'order_date', 'order_status_code']

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
    .withColumn("table_name", lit("bronze_orders"))
    .withColumn("rule_name", lit("Order header completeness"))
    .withColumn("rule_category", lit("Null / Completeness Checks"))
    .withColumn("failure_percentage", round((col("failed_records_count") * 100.0) / col("total_records_checked"), 2))
    .withColumn("run_timestamp", current_timestamp())
    .withColumn("severity", lit("HIGH"))
    .withColumn("status", when(col("failed_records_count") > 0, lit("FAILED")).otherwise(lit("PASSED")))
)

dq_metrics_df.show(truncate=False)

# valid_df.write.format("delta").mode("append").saveAsTable("ecomsphere.silver.silver_fact_order_line_enriched")
# invalid_df.write.format("delta").mode("append").saveAsTable("ecomsphere.silver_quarantine.silver_quarantine.order_header_completeness_rejections")

