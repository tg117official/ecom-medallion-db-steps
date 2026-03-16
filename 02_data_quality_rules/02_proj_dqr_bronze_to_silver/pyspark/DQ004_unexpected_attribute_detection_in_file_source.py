# ============================================================
# DQ004 - Unexpected attribute detection in file source
# Template Type: structural_check
# Severity: MEDIUM
# Category: Schema and Structural Checks
# Auto-generated PySpark practice script
# ============================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.getOrCreate()

dq_run_id = "DQ004_RUN_001"
rule_name = "Unexpected attribute detection in file source"

def validate_structural_rule(df, validation_columns=None, rescued_data_column="_rescued_data"):
    validation_columns = validation_columns or []
    missing_exprs = [when(col(c).isNull(), lit(c)) for c in validation_columns if c in df.columns]

    if validation_columns:
        validated_df = (
            df.withColumn("missing_required_cols", array(*missing_exprs))
              .withColumn("missing_required_cols", expr("filter(missing_required_cols, x -> x is not null)"))
              .withColumn("missing_required_count", size(col("missing_required_cols")))
        )
    else:
        validated_df = (
            df.withColumn("missing_required_cols", array())
              .withColumn("missing_required_count", lit(0))
        )

    if rescued_data_column in df.columns:
        validated_df = validated_df.withColumn(
            "rescued_data_flag",
            when(col(rescued_data_column).isNotNull(), lit(1)).otherwise(lit(0))
        )
    else:
        validated_df = validated_df.withColumn("rescued_data_flag", lit(0))

    validated_df = (
        validated_df.withColumn(
            "structural_issue",
            concat_ws(
                ", ",
                when(col("rescued_data_flag") == 1, lit("_rescued_data_present")),
                when(col("missing_required_count") > 0, concat_ws(", ", col("missing_required_cols")))
            )
        )
        .withColumn(
            "dq_structural_violation_flag",
            when(col("rescued_data_flag") == 1, lit(1))
            .when(col("missing_required_count") > 0, lit(1))
            .otherwise(lit(0))
        )
    )

    return (
        validated_df,
        validated_df.filter(col("dq_structural_violation_flag") == 0),
        validated_df.filter(col("dq_structural_violation_flag") == 1),
    )

validation_columns = ['_rescued_data']

extra_validation_columns_by_table = {}

all_dq_metrics = []

print("=" * 80)
print("PROCESSING bronze_product_catalog_feed")
print("=" * 80)

sample_data = [
    ("CR001","P101","PV101","S001","Nike Shoes",None,"catalog_001.json","2026-03-10 09:00:00","2026-03-10"),
    ("CR002","P102","PV102","S002","Adidas Shirt",'{"unexpected_field":"abc"}',"catalog_002.json","2026-03-10 09:10:00","2026-03-10"),
    ("CR003",None,"PV103","S003","Puma Jacket",None,"catalog_003.json",None,"2026-03-10")
]
schema = """
catalog_record_id string, product_id string, product_variant_id string, seller_id string, listing_title string,
_rescued_data string, _source_file string, _ingest_ts string, bronze_load_date string
"""
df = spark.createDataFrame(sample_data, schema=schema) \
    .withColumn("_ingest_ts", to_timestamp("_ingest_ts")) \
    .withColumn("bronze_load_date", to_date("bronze_load_date"))

# df = spark.table("ecomsphere.bronze.bronze_product_catalog_feed")

table_validation_columns = list(validation_columns)
if "bronze_product_catalog_feed" in extra_validation_columns_by_table:
    table_validation_columns.extend(extra_validation_columns_by_table["bronze_product_catalog_feed"])
table_validation_columns = list(dict.fromkeys(table_validation_columns))

validated_df, valid_df, invalid_df = validate_structural_rule(df, table_validation_columns)

print("VALID ROWS")
valid_df.show(truncate=False)
print("INVALID ROWS")
invalid_df.show(truncate=False)

dq_metrics_df = (
    validated_df.agg(
        count("*").alias("total_records_checked"),
        sum(when(col("dq_structural_violation_flag") == 1, 1).otherwise(0)).alias("failed_records_count"),
        sum(when(col("dq_structural_violation_flag") == 0, 1).otherwise(0)).alias("passed_records_count")
    )
    .withColumn("dq_run_id", lit(dq_run_id))
    .withColumn("table_name", lit("bronze_product_catalog_feed"))
    .withColumn("rule_name", lit(rule_name))
    .withColumn("rule_category", lit("Schema and Structural Checks"))
    .withColumn("failure_percentage", round((col("failed_records_count") * 100.0) / col("total_records_checked"), 2))
    .withColumn("run_timestamp", current_timestamp())
    .withColumn("severity", lit("MEDIUM"))
    .withColumn("status", when(col("failed_records_count") > 0, lit("FAILED")).otherwise(lit("PASSED")))
)

all_dq_metrics.append(dq_metrics_df)

# valid_df.write.format("delta").mode("append").saveAsTable("ecomsphere.silver.multiple")
# invalid_df.write.format("delta").mode("append").saveAsTable("ecomsphere.silver_quarantine.silver_quarantine.unexpected_attribute_rejections")

print("=" * 80)
print("PROCESSING bronze_customer_review_feed")
print("=" * 80)

sample_data = [
    ("R001","O1001","C101","P101",5,None,"review_001.json","2026-03-10 10:00:00","2026-03-10"),
    ("R002","O1002","C102","P102",4,'{"bad_json":"x"}',"review_002.json","2026-03-10 10:10:00","2026-03-10"),
    (None,"O1003","C103","P103",3,None,"review_003.json","2026-03-10 10:20:00",None)
]
schema = """
review_id string, order_id string, customer_id string, product_id string, rating int,
_rescued_data string, _source_file string, _ingest_ts string, bronze_load_date string
"""
df = spark.createDataFrame(sample_data, schema=schema) \
    .withColumn("_ingest_ts", to_timestamp("_ingest_ts")) \
    .withColumn("bronze_load_date", to_date("bronze_load_date"))

# df = spark.table("ecomsphere.bronze.bronze_customer_review_feed")

table_validation_columns = list(validation_columns)
if "bronze_customer_review_feed" in extra_validation_columns_by_table:
    table_validation_columns.extend(extra_validation_columns_by_table["bronze_customer_review_feed"])
table_validation_columns = list(dict.fromkeys(table_validation_columns))

validated_df, valid_df, invalid_df = validate_structural_rule(df, table_validation_columns)

print("VALID ROWS")
valid_df.show(truncate=False)
print("INVALID ROWS")
invalid_df.show(truncate=False)

dq_metrics_df = (
    validated_df.agg(
        count("*").alias("total_records_checked"),
        sum(when(col("dq_structural_violation_flag") == 1, 1).otherwise(0)).alias("failed_records_count"),
        sum(when(col("dq_structural_violation_flag") == 0, 1).otherwise(0)).alias("passed_records_count")
    )
    .withColumn("dq_run_id", lit(dq_run_id))
    .withColumn("table_name", lit("bronze_customer_review_feed"))
    .withColumn("rule_name", lit(rule_name))
    .withColumn("rule_category", lit("Schema and Structural Checks"))
    .withColumn("failure_percentage", round((col("failed_records_count") * 100.0) / col("total_records_checked"), 2))
    .withColumn("run_timestamp", current_timestamp())
    .withColumn("severity", lit("MEDIUM"))
    .withColumn("status", when(col("failed_records_count") > 0, lit("FAILED")).otherwise(lit("PASSED")))
)

all_dq_metrics.append(dq_metrics_df)

# valid_df.write.format("delta").mode("append").saveAsTable("ecomsphere.silver.multiple")
# invalid_df.write.format("delta").mode("append").saveAsTable("ecomsphere.silver_quarantine.silver_quarantine.unexpected_attribute_rejections")

print("=" * 80)
print("PROCESSING bronze_order_event_log")
print("=" * 80)

sample_data = [
    ("E001","O1001","ORDER","2026-03-10 11:00:00",None,"event_001.json","2026-03-10 11:05:00","2026-03-10"),
    ("E002","O1002","PAYMENT","2026-03-10 11:10:00",'{"bad_field":"123"}',"event_002.json","2026-03-10 11:11:00","2026-03-10"),
    ("E003","O1003",None,"2026-03-10 11:20:00",None,"event_003.json",None,"2026-03-10")
]
schema = """
event_id string, order_id string, event_type string, event_timestamp string,
_rescued_data string, _source_file string, _ingest_ts string, bronze_load_date string
"""
df = spark.createDataFrame(sample_data, schema=schema) \
    .withColumn("event_timestamp", to_timestamp("event_timestamp")) \
    .withColumn("_ingest_ts", to_timestamp("_ingest_ts")) \
    .withColumn("bronze_load_date", to_date("bronze_load_date"))

# df = spark.table("ecomsphere.bronze.bronze_order_event_log")

table_validation_columns = list(validation_columns)
if "bronze_order_event_log" in extra_validation_columns_by_table:
    table_validation_columns.extend(extra_validation_columns_by_table["bronze_order_event_log"])
table_validation_columns = list(dict.fromkeys(table_validation_columns))

validated_df, valid_df, invalid_df = validate_structural_rule(df, table_validation_columns)

print("VALID ROWS")
valid_df.show(truncate=False)
print("INVALID ROWS")
invalid_df.show(truncate=False)

dq_metrics_df = (
    validated_df.agg(
        count("*").alias("total_records_checked"),
        sum(when(col("dq_structural_violation_flag") == 1, 1).otherwise(0)).alias("failed_records_count"),
        sum(when(col("dq_structural_violation_flag") == 0, 1).otherwise(0)).alias("passed_records_count")
    )
    .withColumn("dq_run_id", lit(dq_run_id))
    .withColumn("table_name", lit("bronze_order_event_log"))
    .withColumn("rule_name", lit(rule_name))
    .withColumn("rule_category", lit("Schema and Structural Checks"))
    .withColumn("failure_percentage", round((col("failed_records_count") * 100.0) / col("total_records_checked"), 2))
    .withColumn("run_timestamp", current_timestamp())
    .withColumn("severity", lit("MEDIUM"))
    .withColumn("status", when(col("failed_records_count") > 0, lit("FAILED")).otherwise(lit("PASSED")))
)

all_dq_metrics.append(dq_metrics_df)

# valid_df.write.format("delta").mode("append").saveAsTable("ecomsphere.silver.multiple")
# invalid_df.write.format("delta").mode("append").saveAsTable("ecomsphere.silver_quarantine.silver_quarantine.unexpected_attribute_rejections")


if all_dq_metrics:
    final_dq_metrics_df = all_dq_metrics[0]
    for metric_df in all_dq_metrics[1:]:
        final_dq_metrics_df = final_dq_metrics_df.unionByName(metric_df)

    print("FINAL DQ AUDIT")
    final_dq_metrics_df.show(truncate=False)

    # final_dq_metrics_df.write.format("delta").mode("append").saveAsTable(
    #     "ecomsphere.silver.silver_data_quality_audit"
    # )