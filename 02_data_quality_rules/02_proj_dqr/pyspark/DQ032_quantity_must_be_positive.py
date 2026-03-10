# ============================================================
# DQ032 - Quantity must be positive
# Range / Domain Validation Template
# ============================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.getOrCreate()

sample_data = [
    ("OI101", 2, 10, 2),
    ("OI102", 0, 5, 1),
    ("OI103", -1, 8, -2)
]
schema = "id string, quantity int, on_hand_qty int, reserved_qty int"
df = spark.createDataFrame(sample_data, schema=schema)
validated_df = df.withColumn(
    "dq_range_violation_flag",
    when((col("quantity") <= 0) | (col("on_hand_qty") < 0) | (col("reserved_qty") < 0), 1).otherwise(0)
)

print("VALIDATED OUTPUT")
validated_df.show(truncate=False)