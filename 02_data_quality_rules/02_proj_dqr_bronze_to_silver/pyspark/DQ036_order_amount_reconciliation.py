# ============================================================
# DQ036 - Order amount reconciliation
# Range / Domain Validation Template
# ============================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.getOrCreate()

orders_data = [("O1",100,10,5,20,115),("O2",100,10,5,20,120)]
orders_df = spark.createDataFrame(orders_data, "order_id string, total_item_amount double, total_discount_amount double, total_tax_amount double, total_shipping_amount double, grand_total_amount double")
validated_df = orders_df.withColumn(
    "expected_grand_total",
    col("total_item_amount") - col("total_discount_amount") + col("total_tax_amount") + col("total_shipping_amount")
).withColumn(
    "dq_amount_mismatch_flag",
    when(abs(col("expected_grand_total") - col("grand_total_amount")) > 0.01, 1).otherwise(0)
)

print("VALIDATED OUTPUT")
validated_df.show(truncate=False)