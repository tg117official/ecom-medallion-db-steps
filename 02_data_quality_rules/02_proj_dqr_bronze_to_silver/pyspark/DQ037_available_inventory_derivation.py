# ============================================================
# DQ037 - Available inventory derivation
# Enrichment Template
# ============================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.getOrCreate()

inventory_data = [("W1","PV101",10,2),("W2","PV102",5,1)]
df = spark.createDataFrame(inventory_data, "warehouse_id string, product_variant_id string, on_hand_qty int, reserved_qty int")
result_df = df.withColumn("available_qty", col("on_hand_qty") - col("reserved_qty"))

print("ENRICHED OUTPUT")
result_df.show(truncate=False)