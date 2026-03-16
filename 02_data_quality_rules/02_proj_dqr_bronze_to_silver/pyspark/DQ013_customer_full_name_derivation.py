# ============================================================
# DQ013 - Customer full name derivation
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
print("PROCESSING bronze_customer")
print("=" * 80)

sample_data = [
    ("C101"," Amit "," Patil "," AMIT@GMAIL.COM ","+91 98765-43210","active","2026-03-10 09:00:00","2026-03-10 09:05:00",False,"2026-03-10 09:10:00","2026-03-10"),
    ("C102","ravi","kumar"," Ravi@Example.Com ","98765 11111","inactive","2026-03-10 09:00:00","2026-03-10 09:05:00",False,"2026-03-10 09:10:00","2026-03-10")
]
schema = """
customer_id string, first_name string, last_name string, email string, phone_number string, status_code string,
created_at string, updated_at string, is_deleted boolean, _ingest_ts string, bronze_load_date string
"""
df = spark.createDataFrame(sample_data, schema=schema)

# df = spark.table("ecomsphere.bronze.bronze_customer")

result_df = df

result_df = result_df.withColumn("full_name", concat_ws(" ", trim(col("first_name")), trim(col("last_name"))))

print("STANDARDIZED OUTPUT")
result_df.show(truncate=False)

# result_df.write.format("delta").mode("append").saveAsTable("ecomsphere.silver.silver_dim_customer_profile")
