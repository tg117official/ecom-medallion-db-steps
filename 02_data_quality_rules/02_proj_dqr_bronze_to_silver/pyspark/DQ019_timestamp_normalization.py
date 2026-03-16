# ============================================================
# DQ019 - Timestamp normalization
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
print("PROCESSING all_bronze_tables")
print("=" * 80)

sample_data = [(" sample ",)]
schema = "raw_value string"
df = spark.createDataFrame(sample_data, schema=schema)

# df = spark.table("ecomsphere.bronze.all_bronze_tables")

result_df = df

for c in result_df.columns:
    if c.endswith("_at") or c.endswith("_date") or "timestamp" in c:
        result_df = result_df.withColumn(c, to_timestamp(col(c)))

print("STANDARDIZED OUTPUT")
result_df.show(truncate=False)

# result_df.write.format("delta").mode("append").saveAsTable("ecomsphere.silver.multiple")
