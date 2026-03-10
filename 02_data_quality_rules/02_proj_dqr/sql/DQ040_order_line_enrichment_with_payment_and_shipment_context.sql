# ============================================================
# DQ040 - Order line enrichment with payment and shipment context
# Enrichment Template
# ============================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.getOrCreate()

orders = [("O1","C101","2026-03-10 10:00:00","PLACED")]
items = [("OI1","O1","PV101",2,100.0,215.0)]
payments = [("PAY1","O1","UPI","SUCCESS")]
shipments = [("S1","O1","W1","Bluedart","SHIPPED")]
variants = [("PV101","P101")]
orders_df = spark.createDataFrame(orders, "order_id string, customer_id string, order_date string, order_status_code string")
items_df = spark.createDataFrame(items, "order_item_id string, order_id string, product_variant_id string, quantity int, base_unit_price double, line_total_amount double")
payments_df = spark.createDataFrame(payments, "payment_id string, order_id string, payment_method_code string, payment_status_code string")
shipments_df = spark.createDataFrame(shipments, "shipment_id string, order_id string, warehouse_id string, carrier_name string, shipment_status_code string")
variants_df = spark.createDataFrame(variants, "product_variant_id string, product_id string")
result_df = items_df.join(orders_df, "order_id") \
    .join(payments_df, "order_id", "left") \
    .join(shipments_df, "order_id", "left") \
    .join(variants_df, "product_variant_id", "left")

print("ENRICHED OUTPUT")
result_df.show(truncate=False)