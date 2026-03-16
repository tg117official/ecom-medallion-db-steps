# ============================================================
# DQ038 - Verified purchase enrichment for reviews
# Enrichment Template
# ============================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.getOrCreate()

reviews = [("R1","C101","P101","PV101"),("R2","C999","P999","PV999")]
orders = [("O1","C101")]
order_items = [("O1","PV101")]
reviews_df = spark.createDataFrame(reviews, "review_id string, customer_id string, product_id string, product_variant_id string")
orders_df = spark.createDataFrame(orders, "order_id string, customer_id string")
order_items_df = spark.createDataFrame(order_items, "order_id string, product_variant_id string")
verified_pairs = orders_df.join(order_items_df, "order_id").select("customer_id", "product_variant_id").distinct()
result_df = reviews_df.join(verified_pairs, ["customer_id", "product_variant_id"], "left") \
    .withColumn("is_verified_purchase", when(col("product_variant_id").isNotNull(), lit(True)).otherwise(lit(False)))

print("ENRICHED OUTPUT")
result_df.show(truncate=False)