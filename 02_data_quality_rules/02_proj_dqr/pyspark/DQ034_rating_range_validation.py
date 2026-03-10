# ============================================================
# DQ034 - Rating range validation
# Range / Domain Validation Template
# ============================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.getOrCreate()

sample_data = [("R1",5),("R2",0),("R3",6),("R4",3)]
schema = "review_id string, rating int"
df = spark.createDataFrame(sample_data, schema=schema)
validated_df = df.withColumn(
    "dq_range_violation_flag",
    when((col("rating") < 1) | (col("rating") > 5), 1).otherwise(0)
)

print("VALIDATED OUTPUT")
validated_df.show(truncate=False)