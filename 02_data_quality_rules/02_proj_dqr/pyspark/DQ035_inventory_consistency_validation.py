# ============================================================
# DQ035 - Inventory consistency validation
# Range / Domain Validation Template
# ============================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.getOrCreate()

sample_data = [("W1","PV1",10,2),("W2","PV2",5,8),("W3","PV3",7,7)]
schema = "warehouse_id string, product_variant_id string, on_hand_qty int, reserved_qty int"
df = spark.createDataFrame(sample_data, schema=schema)
validated_df = df.withColumn("available_qty", col("on_hand_qty") - col("reserved_qty")) \
    .withColumn("dq_negative_stock_flag", when((col("reserved_qty") > col("on_hand_qty")) | (col("available_qty") < 0), 1).otherwise(0))

print("VALIDATED OUTPUT")
validated_df.show(truncate=False)