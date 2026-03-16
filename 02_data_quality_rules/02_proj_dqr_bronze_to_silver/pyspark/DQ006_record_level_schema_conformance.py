# ============================================================
# DQ006 - Record-level schema conformance
# Record-Level Schema Conformance Template
# ============================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.getOrCreate()

# ------------------------------------------------------------
# Practice data: some rows conform, some don't
# ------------------------------------------------------------
sample_data = [
    ("R001", "O1001", "C101", "P101", "PV101", "5", "2026-03-10 10:00:00"),
    ("R002", "O1002", "C102", "P102", "PV102", "bad_rating", "bad_ts"),
    (None, "O1003", "C103", "P103", "PV103", "3", "2026-03-10 10:30:00")
]

df = spark.createDataFrame(
    sample_data,
    """
    review_id string,
    order_id string,
    customer_id string,
    product_id string,
    product_variant_id string,
    rating_raw string,
    review_created_at_raw string
    """
)

# Uncomment below for production
# df = spark.table("ecomsphere.bronze.<source_table>")

conformed_df = df \
    .withColumn("rating", col("rating_raw").cast("int")) \
    .withColumn("review_created_at", to_timestamp("review_created_at_raw")) \
    .withColumn(
        "dq_schema_conformance_flag",
        when(
            col("review_id").isNull() |
            col("order_id").isNull() |
            col("customer_id").isNull() |
            col("product_id").isNull() |
            col("product_variant_id").isNull() |
            col("rating").isNull() |
            col("review_created_at").isNull(),
            1
        ).otherwise(0)
    )

valid_df = conformed_df.filter(col("dq_schema_conformance_flag") == 0)
invalid_df = conformed_df.filter(col("dq_schema_conformance_flag") == 1)

print("CONFORMING ROWS")
valid_df.show(truncate=False)

print("NON-CONFORMING ROWS")
invalid_df.show(truncate=False)