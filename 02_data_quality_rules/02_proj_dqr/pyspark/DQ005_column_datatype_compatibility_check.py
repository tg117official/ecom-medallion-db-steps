# ============================================================
# DQ005 - Column datatype compatibility check
# Datatype Compatibility Check Template
# ============================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.getOrCreate()

datatype_expectations = {'bronze_customer.email': 'string', 'bronze_customer.created_at': 'timestamp', 'bronze_orders.order_date': 'timestamp', 'bronze_order_item.quantity': 'int', 'bronze_order_item.line_total_amount': 'decimal_or_double', 'bronze_payment.amount': 'decimal_or_double', 'bronze_customer_review_feed.rating': 'int', 'bronze_order_event_log.event_timestamp': 'timestamp', 'bronze_product_catalog_feed.selling_price': 'decimal_or_double'}

# ------------------------------------------------------------
# Build one mixed sample dataset for practice
# ------------------------------------------------------------
sample_data = [
    ("amit@gmail.com", "2026-03-10 09:00:00", "2026-03-10 10:00:00", "2", "215.50", "115.25", "5", "2026-03-10 11:00:00", "4200.00"),
    ("bad_email", "bad_ts", "bad_ts", "abc", "xyz", "-10", "9", "not_ts", "text")
]

df = spark.createDataFrame(
    sample_data,
    """
    email string,
    customer_created_at string,
    order_date string,
    quantity string,
    line_total_amount string,
    payment_amount string,
    rating string,
    event_timestamp string,
    selling_price string
    """
)

# Uncomment below and replace with actual table-specific logic in production
# df = spark.table("ecomsphere.bronze.<your_table>")

validated_df = df \
    .withColumn("email_cast_ok", col("email").cast("string").isNotNull()) \
    .withColumn("customer_created_at_cast_ok", to_timestamp("customer_created_at").isNotNull()) \
    .withColumn("order_date_cast_ok", to_timestamp("order_date").isNotNull()) \
    .withColumn("quantity_cast_ok", col("quantity").cast("int").isNotNull()) \
    .withColumn("line_total_amount_cast_ok", col("line_total_amount").cast("double").isNotNull()) \
    .withColumn("payment_amount_cast_ok", col("payment_amount").cast("double").isNotNull()) \
    .withColumn("rating_cast_ok", col("rating").cast("int").isNotNull()) \
    .withColumn("event_timestamp_cast_ok", to_timestamp("event_timestamp").isNotNull()) \
    .withColumn("selling_price_cast_ok", col("selling_price").cast("double").isNotNull()) \
    .withColumn(
        "dq_datatype_violation_flag",
        when(
            (~col("customer_created_at_cast_ok")) |
            (~col("order_date_cast_ok")) |
            (~col("quantity_cast_ok")) |
            (~col("line_total_amount_cast_ok")) |
            (~col("payment_amount_cast_ok")) |
            (~col("rating_cast_ok")) |
            (~col("event_timestamp_cast_ok")) |
            (~col("selling_price_cast_ok")),
            1
        ).otherwise(0)
    )

print("DATATYPE VALIDATION OUTPUT")
validated_df.show(truncate=False)