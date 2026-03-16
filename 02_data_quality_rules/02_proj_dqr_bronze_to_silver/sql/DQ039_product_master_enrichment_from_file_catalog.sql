# ============================================================
# DQ039 - Product master enrichment from file catalog
# Enrichment Template
# ============================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.getOrCreate()

product = [("P101","Shoe","B101","CAT101","desc")]
variant = [("PV101","P101","SKU101","Red 9")]
brand = [("B101","Nike")]
category = [("CAT101","Shoes",None,1)]
catalog = [("CR1","P101","PV101","S001","Seller1","Nike Shoes","Great","Black","9","Mesh",12,"IN","640411",5000.0,4200.0,20)]
product_df = spark.createDataFrame(product, "product_id string, product_name string, brand_id string, default_category_id string, short_description string")
variant_df = spark.createDataFrame(variant, "product_variant_id string, product_id string, sku string, variant_name string")
brand_df = spark.createDataFrame(brand, "brand_id string, brand_name string")
category_df = spark.createDataFrame(category, "category_id string, category_name string, parent_category_id string, category_level int")
catalog_df = spark.createDataFrame(catalog, """
catalog_record_id string, product_id string, product_variant_id string, seller_id string, seller_name string,
listing_title string, listing_description string, color string, size string, material string, warranty_months int,
country_of_origin string, hsn_code string, mrp double, selling_price double, available_stock int
""")
result_df = variant_df.join(product_df, "product_id") \
    .join(brand_df, "brand_id", "left") \
    .join(category_df, product_df.default_category_id == category_df.category_id, "left") \
    .join(catalog_df.select("product_id","product_variant_id","seller_id","seller_name","listing_title","listing_description","color","size","material","warranty_months","country_of_origin","hsn_code","mrp","selling_price","available_stock"), ["product_id","product_variant_id"], "left")

print("ENRICHED OUTPUT")
result_df.show(truncate=False)