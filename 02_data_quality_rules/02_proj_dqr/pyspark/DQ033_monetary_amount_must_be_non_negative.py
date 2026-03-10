# ============================================================
# DQ033 - Monetary amount must be non-negative
# Range / Domain Validation Template
# ============================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.getOrCreate()

sample_data = [
    ("A1",100.0,10.0,5.0,20.0),
    ("A2",-1.0,10.0,5.0,20.0),
    ("A3",100.0,-2.0,5.0,-1.0)
]
schema = "id string, amount1 double, amount2 double, amount3 double, amount4 double"
df = spark.createDataFrame(sample_data, schema=schema)
validated_df = df.withColumn(
    "dq_range_violation_flag",
    when((col("amount1") < 0) | (col("amount2") < 0) | (col("amount3") < 0) | (col("amount4") < 0), 1).otherwise(0)
)

print("VALIDATED OUTPUT")
validated_df.show(truncate=False)