# ============================================================
# DQ025 - Order-to-customer reference integrity
# Reference Integrity Template
# ============================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.getOrCreate()

child_table = "bronze_orders"
parent_table = "bronze_customer"
child_column = "customer_id"
parent_column = "customer_id"

if child_table == "bronze_orders" and parent_table == "bronze_customer":
    child_data = [("O1001","C101"),("O1002","C999"),("O1003","C102")]
    parent_data = [("C101",),("C102",)]
    child_schema = "order_id string, customer_id string"
    parent_schema = "customer_id string"
elif child_table == "bronze_order_item" and parent_table == "bronze_orders":
    child_data = [("OI101","O1001"),("OI102","O999"),("OI103","O1002")]
    parent_data = [("O1001",),("O1002",)]
    child_schema = "order_item_id string, order_id string"
    parent_schema = "order_id string"
elif child_table == "bronze_order_item" and parent_table == "bronze_product_variant":
    child_data = [("OI101","PV101"),("OI102","PV999"),("OI103","PV102")]
    parent_data = [("PV101",),("PV102",)]
    child_schema = "order_item_id string, product_variant_id string"
    parent_schema = "product_variant_id string"
elif child_table == "bronze_payment" and parent_table == "bronze_orders":
    child_data = [("PAY101","O1001"),("PAY102","O999"),("PAY103","O1002")]
    parent_data = [("O1001",),("O1002",)]
    child_schema = "payment_id string, order_id string"
    parent_schema = "order_id string"
elif child_table == "bronze_shipment" and parent_table == "bronze_orders":
    child_data = [("S101","O1001"),("S102","O999"),("S103","O1002")]
    parent_data = [("O1001",),("O1002",)]
    child_schema = "shipment_id string, order_id string"
    parent_schema = "order_id string"
elif child_table == "bronze_inventory_level" and parent_table == "bronze_product_variant":
    child_data = [("W1","PV101"),("W2","PV999"),("W3","PV102")]
    parent_data = [("PV101",),("PV102",)]
    child_schema = "warehouse_id string, product_variant_id string"
    parent_schema = "product_variant_id string"
elif child_table == "bronze_product_catalog_feed" and parent_table == "bronze_product_variant":
    child_data = [("CR001","P101","PV101"),("CR002","P999","PV999"),("CR003","P102","PV102")]
    parent_data = [("PV101",),("PV102",)]
    child_schema = "catalog_record_id string, product_id string, product_variant_id string"
    parent_schema = "product_variant_id string"
else:
    child_data = [("K1","A1"),("K2","A9")]
    parent_data = [("A1",)]
    child_schema = "id string, ref_id string"
    parent_schema = "ref_id string"

child_df = spark.createDataFrame(child_data, schema=child_schema)
parent_df = spark.createDataFrame(parent_data, schema=parent_schema)

invalid_df = child_df.alias("c").join(
    parent_df.alias("p"),
    col(f"c.{child_column}") == col(f"p.{parent_column}"),
    "left_anti"
)

valid_df = child_df.alias("c").join(
    parent_df.alias("p"),
    col(f"c.{child_column}") == col(f"p.{parent_column}"),
    "inner"
).select("c.*")

print("VALID ROWS")
valid_df.show(truncate=False)
print("REFERENCE VIOLATIONS")
invalid_df.show(truncate=False)


# valid_df.write.format("delta").mode("append").saveAsTable("ecomsphere.silver.silver_fact_order_line_enriched")
# invalid_df.write.format("delta").mode("append").saveAsTable("ecomsphere.silver_quarantine.silver_quarantine.order_customer_reference_rejections")