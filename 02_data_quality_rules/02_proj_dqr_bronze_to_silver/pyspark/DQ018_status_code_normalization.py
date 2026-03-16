# ============================================================
# DQ018 - Status code normalization
# Standardization Template
# ============================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import re

spark = SparkSession.builder.getOrCreate()

def standardize_phone_expr(column_name):
    return regexp_replace(col(column_name), "[^0-9]", "")

print("=" * 80)
print("PROCESSING bronze_orders")
print("=" * 80)

sample_data = [(" sample ",)]
schema = "raw_value string"
df = spark.createDataFrame(sample_data, schema=schema)

# df = spark.table("ecomsphere.bronze.bronze_orders")

result_df = df

status_map = create_map(
    [lit("active"), lit("ACTIVE"),
     lit("inactive"), lit("INACTIVE"),
     lit("placed"), lit("PLACED"),
     lit("success"), lit("SUCCESS"),
     lit("approved"), lit("APPROVED")]
)
for c in result_df.columns:
    if "status" in c:
        result_df = result_df.withColumn(c, coalesce(status_map[lower(trim(col(c)))], upper(trim(col(c)))))

print("STANDARDIZED OUTPUT")
result_df.show(truncate=False)

# result_df.write.format("delta").mode("append").saveAsTable("ecomsphere.silver.multiple")
print("=" * 80)
print("PROCESSING bronze_payment")
print("=" * 80)

sample_data = [(" sample ",)]
schema = "raw_value string"
df = spark.createDataFrame(sample_data, schema=schema)

# df = spark.table("ecomsphere.bronze.bronze_payment")

result_df = df

status_map = create_map(
    [lit("active"), lit("ACTIVE"),
     lit("inactive"), lit("INACTIVE"),
     lit("placed"), lit("PLACED"),
     lit("success"), lit("SUCCESS"),
     lit("approved"), lit("APPROVED")]
)
for c in result_df.columns:
    if "status" in c:
        result_df = result_df.withColumn(c, coalesce(status_map[lower(trim(col(c)))], upper(trim(col(c)))))

print("STANDARDIZED OUTPUT")
result_df.show(truncate=False)

# result_df.write.format("delta").mode("append").saveAsTable("ecomsphere.silver.multiple")
print("=" * 80)
print("PROCESSING bronze_shipment")
print("=" * 80)

sample_data = [(" sample ",)]
schema = "raw_value string"
df = spark.createDataFrame(sample_data, schema=schema)

# df = spark.table("ecomsphere.bronze.bronze_shipment")

result_df = df

status_map = create_map(
    [lit("active"), lit("ACTIVE"),
     lit("inactive"), lit("INACTIVE"),
     lit("placed"), lit("PLACED"),
     lit("success"), lit("SUCCESS"),
     lit("approved"), lit("APPROVED")]
)
for c in result_df.columns:
    if "status" in c:
        result_df = result_df.withColumn(c, coalesce(status_map[lower(trim(col(c)))], upper(trim(col(c)))))

print("STANDARDIZED OUTPUT")
result_df.show(truncate=False)

# result_df.write.format("delta").mode("append").saveAsTable("ecomsphere.silver.multiple")
print("=" * 80)
print("PROCESSING bronze_customer_review_feed")
print("=" * 80)

sample_data = [(" sample ",)]
schema = "raw_value string"
df = spark.createDataFrame(sample_data, schema=schema)

# df = spark.table("ecomsphere.bronze.bronze_customer_review_feed")

result_df = df

status_map = create_map(
    [lit("active"), lit("ACTIVE"),
     lit("inactive"), lit("INACTIVE"),
     lit("placed"), lit("PLACED"),
     lit("success"), lit("SUCCESS"),
     lit("approved"), lit("APPROVED")]
)
for c in result_df.columns:
    if "status" in c:
        result_df = result_df.withColumn(c, coalesce(status_map[lower(trim(col(c)))], upper(trim(col(c)))))

print("STANDARDIZED OUTPUT")
result_df.show(truncate=False)

# result_df.write.format("delta").mode("append").saveAsTable("ecomsphere.silver.multiple")
