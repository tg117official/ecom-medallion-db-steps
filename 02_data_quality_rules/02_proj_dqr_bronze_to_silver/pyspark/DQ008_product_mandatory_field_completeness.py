# ============================================================
# DQ008 - Product mandatory field completeness
# Completeness Template
# ============================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.getOrCreate()

dq_run_id = "DQ008_RUN_001"

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
print("PROCESSING bronze_product")
print("=" * 80)

sample_data = [
    ("P101","Shoe","B101","CAT101","desc","2026-03-10 09:00:00","2026-03-10 09:10:00",True,False,"2026-03-10 09:15:00","2026-03-10"),
    ("P102",None,"B102","CAT102","desc","2026-03-10 09:00:00","2026-03-10 09:10:00",True,False,"2026-03-10 09:15:00","2026-03-10"),
    ("P103","Bag",None,None,"desc","2026-03-10 09:00:00","2026-03-10 09:10:00",True,False,"2026-03-10 09:15:00","2026-03-10")
]
schema = """
product_id string, product_name string, brand_id string, default_category_id string, short_description string,
created_at string, updated_at string, is_active boolean, is_deleted boolean, _ingest_ts string, bronze_load_date string
"""
df = spark.createDataFrame(sample_data, schema=schema) \
    .withColumn("created_at", to_timestamp("created_at")) \
    .withColumn("updated_at", to_timestamp("updated_at")) \
    .withColumn("_ingest_ts", to_timestamp("_ingest_ts")) \
    .withColumn("bronze_load_date", to_date("bronze_load_date"))

# df = spark.table("ecomsphere.bronze.bronze_product")

validation_columns = ['product_id', 'product_name', 'brand_id', 'default_category_id']

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
    .withColumn("table_name", lit("bronze_product"))
    .withColumn("rule_name", lit("Product mandatory field completeness"))
    .withColumn("rule_category", lit("Null / Completeness Checks"))
    .withColumn("failure_percentage", round((col("failed_records_count") * 100.0) / col("total_records_checked"), 2))
    .withColumn("run_timestamp", current_timestamp())
    .withColumn("severity", lit("HIGH"))
    .withColumn("status", when(col("failed_records_count") > 0, lit("FAILED")).otherwise(lit("PASSED")))
)

dq_metrics_df.show(truncate=False)

# valid_df.write.format("delta").mode("append").saveAsTable("ecomsphere.silver.silver_dim_product_master")
# invalid_df.write.format("delta").mode("append").saveAsTable("ecomsphere.silver_quarantine.silver_quarantine.product_completeness_rejections")

print("=" * 80)
print("PROCESSING bronze_product_variant")
print("=" * 80)

sample_data = [
    ("PV101","P101","SKU101","Red 9","2026-03-10 09:00:00","2026-03-10 09:10:00",True,False,"2026-03-10 09:15:00","2026-03-10"),
    ("PV102","P102",None,"Blue L","2026-03-10 09:00:00","2026-03-10 09:10:00",True,False,"2026-03-10 09:15:00","2026-03-10"),
    (None,"P103","SKU103",None,"2026-03-10 09:00:00","2026-03-10 09:10:00",True,False,"2026-03-10 09:15:00","2026-03-10")
]
schema = """
product_variant_id string, product_id string, sku string, variant_name string,
created_at string, updated_at string, is_active boolean, is_deleted boolean, _ingest_ts string, bronze_load_date string
"""
df = spark.createDataFrame(sample_data, schema=schema) \
    .withColumn("created_at", to_timestamp("created_at")) \
    .withColumn("updated_at", to_timestamp("updated_at")) \
    .withColumn("_ingest_ts", to_timestamp("_ingest_ts")) \
    .withColumn("bronze_load_date", to_date("bronze_load_date"))

# df = spark.table("ecomsphere.bronze.bronze_product_variant")

validation_columns = ['product_variant_id', 'product_id', 'sku', 'variant_name']

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
    .withColumn("table_name", lit("bronze_product_variant"))
    .withColumn("rule_name", lit("Product mandatory field completeness"))
    .withColumn("rule_category", lit("Null / Completeness Checks"))
    .withColumn("failure_percentage", round((col("failed_records_count") * 100.0) / col("total_records_checked"), 2))
    .withColumn("run_timestamp", current_timestamp())
    .withColumn("severity", lit("HIGH"))
    .withColumn("status", when(col("failed_records_count") > 0, lit("FAILED")).otherwise(lit("PASSED")))
)

dq_metrics_df.show(truncate=False)

# valid_df.write.format("delta").mode("append").saveAsTable("ecomsphere.silver.silver_dim_product_master")
# invalid_df.write.format("delta").mode("append").saveAsTable("ecomsphere.silver_quarantine.silver_quarantine.product_completeness_rejections")

