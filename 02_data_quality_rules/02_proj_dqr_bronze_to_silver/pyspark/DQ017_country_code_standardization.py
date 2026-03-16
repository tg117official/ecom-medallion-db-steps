# ============================================================
# DQ017 - Country code standardization
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
print("PROCESSING bronze_customer_address")
print("=" * 80)

sample_data = [("Nagpur "), (" mumbai"), ("DELHI")]
schema = "city string"
df = spark.createDataFrame(sample_data, schema=schema)

# df = spark.table("ecomsphere.bronze.bronze_customer_address")

result_df = df

if "country_code" in result_df.columns:
    result_df = result_df.withColumn("country_code", upper(trim(col("country_code"))))
if "country_of_origin" in result_df.columns:
    result_df = result_df.withColumn("country_of_origin", upper(trim(col("country_of_origin"))))

print("STANDARDIZED OUTPUT")
result_df.show(truncate=False)

# result_df.write.format("delta").mode("append").saveAsTable("ecomsphere.silver.multiple")
print("=" * 80)
print("PROCESSING bronze_warehouse")
print("=" * 80)

sample_data = [("Nagpur "), (" mumbai"), ("DELHI")]
schema = "city string"
df = spark.createDataFrame(sample_data, schema=schema)

# df = spark.table("ecomsphere.bronze.bronze_warehouse")

result_df = df

if "country_code" in result_df.columns:
    result_df = result_df.withColumn("country_code", upper(trim(col("country_code"))))
if "country_of_origin" in result_df.columns:
    result_df = result_df.withColumn("country_of_origin", upper(trim(col("country_of_origin"))))

print("STANDARDIZED OUTPUT")
result_df.show(truncate=False)

# result_df.write.format("delta").mode("append").saveAsTable("ecomsphere.silver.multiple")
print("=" * 80)
print("PROCESSING bronze_product_catalog_feed")
print("=" * 80)

sample_data = [(" sample ",)]
schema = "raw_value string"
df = spark.createDataFrame(sample_data, schema=schema)

# df = spark.table("ecomsphere.bronze.bronze_product_catalog_feed")

result_df = df

if "country_code" in result_df.columns:
    result_df = result_df.withColumn("country_code", upper(trim(col("country_code"))))
if "country_of_origin" in result_df.columns:
    result_df = result_df.withColumn("country_of_origin", upper(trim(col("country_of_origin"))))

print("STANDARDIZED OUTPUT")
result_df.show(truncate=False)

# result_df.write.format("delta").mode("append").saveAsTable("ecomsphere.silver.multiple")
