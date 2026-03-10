# ============================================================
# PYSPARK HANDS-ON SCRIPT
# Covers first 5 sections:
# 1. Data Transformation
# 2. Data Cleansing
# 3. Data Validation Rules
# 4. Data Profiling and Analysis
# 5. Data Integrity Checks
#
# DataFrames are created from Python variables only.
# ============================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, upper, initcap, lower, regexp_replace,
    to_date, when, lit, count, isnan, sum as spark_sum,
    avg, min as spark_min, max as spark_max,
    countDistinct, row_number
)
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# ------------------------------------------------------------
# 0. SPARK SESSION
# ------------------------------------------------------------
spark = SparkSession.builder \
    .appName("ETL_Terminologies_HandsOn") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# ------------------------------------------------------------
# 1. SOURCE DATA (CREATED FROM PYTHON VARIABLES)
# ------------------------------------------------------------
customer_data = [
    (101, " john doe ", "JOHN@EXAMPLE.COM", "india", "9876543210", "12/31/2024", 25, 50000.0),
    (101, " john doe ", "JOHN@EXAMPLE.COM", "india", "9876543210", "12/31/2024", 25, 50000.0),  # exact duplicate
    (102, "JANE DOE", "janeexample.com", "IN", "09876543210", "11/15/2024", 28, 60000.0),       # invalid email
    (103, "alice", "alice@example.com", "Bharat", None, "10/10/2024", None, 70000.0),            # null phone, null age
    (104, "Bob Smith", "bob@example.com", "India", "9123456780", "09/05/2024", 150, 80000.0),    # invalid age
    (105, "charlie", "charlie@example.com", "India", "98765-43210", "13/40/2024", 30, 9999999.0),# invalid date, outlier salary
    (106, "david", None, "USA", "123456789", "08/01/2024", 35, -5000.0),                          # null email, invalid phone, negative salary
]

product_data = [
    ("P101", "Laptop", "Electronics"),
    ("P102", "Phone", "Electronics"),
    ("P103", "Desk", "Furniture"),
]

order_data = [
    (1001, 101, "P101", 2, 100000.0),
    (1002, 102, "P102", 1, 25000.0),
    (1003, 107, "P999", 1, 9999.0),   # invalid customer_id and invalid product_id
    (1004, 103, "P103", 3, 15000.0),
    (1005, None, "P101", 1, 50000.0), # null customer_id
]

customer_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("full_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("country", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("join_date_raw", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("salary", DoubleType(), True),
])

product_schema = StructType([
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
])

order_schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("product_id", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("order_amount", DoubleType(), True),
])

customers_df = spark.createDataFrame(customer_data, customer_schema)
products_df = spark.createDataFrame(product_data, product_schema)
orders_df = spark.createDataFrame(order_data, order_schema)

customers_df.cache()
products_df.cache()
orders_df.cache()

print("\n================ ORIGINAL CUSTOMERS DATA ================\n")
customers_df.show(truncate=False)

print("\n================ ORIGINAL PRODUCTS DATA ================\n")
products_df.show(truncate=False)

print("\n================ ORIGINAL ORDERS DATA ================\n")
orders_df.show(truncate=False)

# ============================================================
# SECTION 1: DATA TRANSFORMATION
# ============================================================
print("\n========================================================")
print("SECTION 1: DATA TRANSFORMATION")
print("Exercises:")
print("1. Standardize names, emails, country, phone, date")
print("2. Derive first_name and last_name from full_name")
print("3. Add salary band")
print("========================================================\n")

transformed_df = customers_df \
    .withColumn("full_name", initcap(trim(col("full_name")))) \
    .withColumn("email", lower(trim(col("email")))) \
    .withColumn(
        "country",
        when(lower(trim(col("country"))).isin("in", "india", "bharat"), lit("India"))
        .when(lower(trim(col("country"))) == "usa", lit("USA"))
        .otherwise(initcap(trim(col("country"))))
    ) \
    .withColumn("phone_digits", regexp_replace(col("phone"), "[^0-9]", "")) \
    .withColumn(
        "phone_standardized",
        when(col("phone_digits").isNull(), None)
        .when((col("phone_digits").startswith("0")) & (col("phone_digits").rlike("^0[0-9]{10}$")),
              regexp_replace(col("phone_digits"), "^0", "+91-"))
        .when(col("phone_digits").rlike("^[0-9]{10}$"), lit("+91-") + col("phone_digits"))
        .otherwise(col("phone_digits"))
    ) \
    .withColumn("join_date", to_date(col("join_date_raw"), "MM/dd/yyyy")) \
    .withColumn("first_name", regexp_replace(trim(col("full_name")), "^([^ ]+).*$", "$1")) \
    .withColumn("last_name", regexp_replace(trim(col("full_name")), "^[^ ]+ ?", "")) \
    .withColumn(
        "salary_band",
        when(col("salary") < 30000, "Low")
        .when((col("salary") >= 30000) & (col("salary") < 80000), "Medium")
        .otherwise("High")
    )

print("Transformed DataFrame:")
transformed_df.show(truncate=False)

# ============================================================
# SECTION 2: DATA CLEANSING
# Covers:
# - Standardization
# - De-duplication
# - Outlier Detection / Handling
# ============================================================
print("\n========================================================")
print("SECTION 2: DATA CLEANSING")
print("Exercises:")
print("1. Remove duplicates")
print("2. Standardize values")
print("3. Detect outliers in age and salary")
print("4. Handle outliers using capping")
print("========================================================\n")

# 2.1 Deduplication
dedup_df = transformed_df.dropDuplicates()

print("After Deduplication:")
dedup_df.show(truncate=False)

# 2.2 Outlier Detection (simple business-rule based)
outlier_flag_df = dedup_df \
    .withColumn(
        "age_outlier_flag",
        when((col("age") < 0) | (col("age") > 120), lit("Y")).otherwise(lit("N"))
    ) \
    .withColumn(
        "salary_outlier_flag",
        when((col("salary") < 0) | (col("salary") > 500000), lit("Y")).otherwise(lit("N"))
    )

print("Detected Outliers:")
outlier_flag_df.select(
    "customer_id", "full_name", "age", "salary", "age_outlier_flag", "salary_outlier_flag"
).show(truncate=False)

# 2.3 Outlier Handling (capping/flooring)
cleansed_df = outlier_flag_df \
    .withColumn(
        "age_cleansed",
        when(col("age") < 0, 0)
        .when(col("age") > 120, 120)
        .otherwise(col("age"))
    ) \
    .withColumn(
        "salary_cleansed",
        when(col("salary") < 0, 0.0)
        .when(col("salary") > 500000, 500000.0)
        .otherwise(col("salary"))
    )

print("After Handling Outliers:")
cleansed_df.select(
    "customer_id", "full_name", "age", "age_cleansed", "salary", "salary_cleansed"
).show(truncate=False)

# ============================================================
# SECTION 3: DATA VALIDATION RULES
# ============================================================
print("\n========================================================")
print("SECTION 3: DATA VALIDATION RULES")
print("Exercises:")
print("1. Validate email format")
print("2. Validate age range")
print("3. Validate date conversion")
print("4. Validate phone length")
print("5. Validate nulls in mandatory fields")
print("========================================================\n")

validated_df = cleansed_df \
    .withColumn(
        "email_valid",
        when(col("email").rlike(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"), "Y").otherwise("N")
    ) \
    .withColumn(
        "age_valid",
        when((col("age_cleansed") >= 0) & (col("age_cleansed") <= 120), "Y").otherwise("N")
    ) \
    .withColumn(
        "join_date_valid",
        when(col("join_date").isNotNull(), "Y").otherwise("N")
    ) \
    .withColumn(
        "phone_valid",
        when(col("phone_digits").rlike(r"^[0-9]{10,11}$"), "Y").otherwise("N")
    ) \
    .withColumn(
        "mandatory_fields_valid",
        when(
            col("customer_id").isNotNull() &
            col("full_name").isNotNull() &
            col("email").isNotNull(),
            "Y"
        ).otherwise("N")
    )

print("Validation Result:")
validated_df.select(
    "customer_id", "full_name", "email", "age_cleansed", "join_date", "phone_digits",
    "email_valid", "age_valid", "join_date_valid", "phone_valid", "mandatory_fields_valid"
).show(truncate=False)

print("Invalid Records Only:")
invalid_records_df = validated_df.filter(
    (col("email_valid") == "N") |
    (col("age_valid") == "N") |
    (col("join_date_valid") == "N") |
    (col("phone_valid") == "N") |
    (col("mandatory_fields_valid") == "N")
)
invalid_records_df.show(truncate=False)

# ============================================================
# SECTION 4: DATA PROFILING AND ANALYSIS
# ============================================================
print("\n========================================================")
print("SECTION 4: DATA PROFILING AND ANALYSIS")
print("Exercises:")
print("1. Get row count")
print("2. Get null count per important column")
print("3. Get distinct counts")
print("4. Get min/max/avg for age and salary")
print("5. Analyze country distribution")
print("========================================================\n")

# 4.1 Row count
print(f"Total rows after cleansing: {validated_df.count()}")

# 4.2 Null counts
null_profile_df = validated_df.select([
    spark_sum(col(c).isNull().cast("int")).alias(c + "_null_count")
    for c in ["customer_id", "full_name", "email", "country", "phone_standardized", "join_date", "age_cleansed", "salary_cleansed"]
])

print("\nNull Profile:")
null_profile_df.show(truncate=False)

# 4.3 Distinct counts
distinct_profile_df = validated_df.select(
    countDistinct("customer_id").alias("distinct_customer_ids"),
    countDistinct("email").alias("distinct_emails"),
    countDistinct("country").alias("distinct_countries")
)

print("Distinct Counts:")
distinct_profile_df.show(truncate=False)

# 4.4 Statistics
stats_df = validated_df.select(
    spark_min("age_cleansed").alias("min_age"),
    spark_max("age_cleansed").alias("max_age"),
    avg("age_cleansed").alias("avg_age"),
    spark_min("salary_cleansed").alias("min_salary"),
    spark_max("salary_cleansed").alias("max_salary"),
    avg("salary_cleansed").alias("avg_salary")
)

print("Basic Statistics:")
stats_df.show(truncate=False)

# 4.5 Distribution
print("Country Distribution:")
validated_df.groupBy("country").count().orderBy(col("count").desc()).show(truncate=False)

print("Salary Band Distribution:")
validated_df.groupBy("salary_band").count().orderBy(col("count").desc()).show(truncate=False)

# ============================================================
# SECTION 5: DATA INTEGRITY CHECKS
# Covers:
# - Entity Integrity
# - Referential Integrity
# - Domain Integrity
# - User-defined Integrity
# ============================================================
print("\n========================================================")
print("SECTION 5: DATA INTEGRITY CHECKS")
print("Exercises:")
print("1. Entity integrity -> customer_id/order_id should be unique and not null")
print("2. Referential integrity -> order customer_id must exist in customers")
print("3. Referential integrity -> order product_id must exist in products")
print("4. Domain integrity -> quantity > 0, order_amount > 0")
print("5. User-defined integrity -> order_amount should be reasonable")
print("========================================================\n")

# 5.1 ENTITY INTEGRITY CHECK - customers
customer_entity_check = validated_df.groupBy("customer_id").count().filter(
    (col("customer_id").isNull()) | (col("count") > 1)
)

print("Customer Entity Integrity Violations:")
customer_entity_check.show(truncate=False)

# 5.2 ENTITY INTEGRITY CHECK - orders
order_entity_check = orders_df.groupBy("order_id").count().filter(
    (col("order_id").isNull()) | (col("count") > 1)
)

print("Order Entity Integrity Violations:")
order_entity_check.show(truncate=False)

# 5.3 REFERENTIAL INTEGRITY CHECK - orders.customer_id exists in customers
invalid_order_customer_ref = orders_df.alias("o").join(
    validated_df.select("customer_id").distinct().alias("c"),
    col("o.customer_id") == col("c.customer_id"),
    "left"
).filter(col("c.customer_id").isNull())

print("Orders with Invalid Customer Reference:")
invalid_order_customer_ref.select("o.*").show(truncate=False)

# 5.4 REFERENTIAL INTEGRITY CHECK - orders.product_id exists in products
invalid_order_product_ref = orders_df.alias("o").join(
    products_df.alias("p"),
    col("o.product_id") == col("p.product_id"),
    "left"
).filter(col("p.product_id").isNull())

print("Orders with Invalid Product Reference:")
invalid_order_product_ref.select("o.*").show(truncate=False)

# 5.5 DOMAIN INTEGRITY CHECK
domain_violations_df = orders_df.filter(
    (col("quantity") <= 0) |
    (col("order_amount") <= 0)
)

print("Domain Integrity Violations in Orders:")
domain_violations_df.show(truncate=False)

# 5.6 USER-DEFINED INTEGRITY CHECK
# Example business rule:
# If quantity = 1, order_amount should not be absurdly high (> 500000)
user_defined_violations_df = orders_df.filter(
    (col("quantity") == 1) & (col("order_amount") > 500000)
)

print("User-defined Integrity Violations:")
user_defined_violations_df.show(truncate=False)

# ------------------------------------------------------------
# OPTIONAL: FINAL CLEAN DATASET FOR FURTHER USE
# ------------------------------------------------------------
final_customers_df = validated_df.select(
    "customer_id",
    "first_name",
    "last_name",
    "full_name",
    "email",
    "country",
    "phone_standardized",
    "join_date",
    "age_cleansed",
    "salary_cleansed",
    "salary_band",
    "email_valid",
    "age_valid",
    "join_date_valid",
    "phone_valid",
    "mandatory_fields_valid"
)

print("\n================ FINAL CUSTOMER DATASET ================\n")
final_customers_df.show(truncate=False)

# ------------------------------------------------------------
# STOP SESSION
# ------------------------------------------------------------
spark.stop()



######################################################################################
######################################################################################

# ============================================================
# PYSPARK HANDS-ON SCRIPT - PART 2
# Covers next 5 sections:
# 6. Cross Referencing
# 7. Data Quality Dashboards
# 8. Normalization
# 9. Data Enrichment
# 10. Data Aggregation
#
# DataFrames are created from Python variables only.
# ============================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, count, sum as spark_sum, countDistinct,
    avg, min as spark_min, max as spark_max,
    regexp_replace, trim, initcap, lower, concat_ws,
    to_date, month, year
)
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, DoubleType
)

# ------------------------------------------------------------
# 0. SPARK SESSION
# ------------------------------------------------------------
spark = SparkSession.builder \
    .appName("ETL_Terminologies_HandsOn_Part2") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# ------------------------------------------------------------
# 1. SOURCE DATA (CREATED FROM PYTHON VARIABLES)
# ------------------------------------------------------------

# Customer master
customer_data = [
    (101, "Rahul Sharma", "rahul@gmail.com", "Nagpur", "Maharashtra"),
    (102, "Priya Patil", "priya@gmail.com", "Pune", "Maharashtra"),
    (103, "Amit Verma", "amit@gmail.com", "Indore", "Madhya Pradesh"),
    (104, "Sneha Joshi", "sneha@gmail.com", "Mumbai", "Maharashtra"),
]

customer_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("customer_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
])

# Product master
product_data = [
    ("P101", "Laptop", "Electronics", 55000.0),
    ("P102", "Mobile", "Electronics", 25000.0),
    ("P103", "Table", "Furniture", 7000.0),
    ("P104", "Chair", "Furniture", 3000.0),
]

product_schema = StructType([
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("unit_price", DoubleType(), True),
])

# Orders with some mismatches and quality issues
order_data = [
    (1001, 101, "P101", "2024-12-01", 1, 55000.0),
    (1002, 102, "P102", "2024-12-01", 2, 50000.0),
    (1003, 103, "P999", "2024-12-02", 1, 9999.0),     # invalid product
    (1004, 999, "P103", "2024-12-02", 3, 21000.0),    # invalid customer
    (1005, 104, "P104", "2024-12-03", None, 3000.0),  # null quantity
    (1006, 101, "P102", "2024-12-03", 2, None),       # null amount
    (1007, 102, "P103", "2024-12-04", 1, 7000.0),
    (1008, 103, "P101", "2024-12-04", 1, 55000.0),
]

order_schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("product_id", StringType(), True),
    StructField("order_date_raw", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("order_amount", DoubleType(), True),
])

# Product catalog from another source for cross-checking product name consistency
external_catalog_data = [
    ("P101", "Laptop", "Electronics"),
    ("P102", "Mobile Phone", "Electronics"),  # mismatch in product name
    ("P103", "Table", "Furniture"),
    ("P104", "Chair", "Furniture"),
]

external_catalog_schema = StructType([
    StructField("product_id", StringType(), True),
    StructField("catalog_product_name", StringType(), True),
    StructField("catalog_category", StringType(), True),
])

# Pincode / geo lookup for enrichment
geo_lookup_data = [
    ("Nagpur", "440001", "Tier-2"),
    ("Pune", "411001", "Metro"),
    ("Indore", "452001", "Tier-2"),
    ("Mumbai", "400001", "Metro"),
]

geo_lookup_schema = StructType([
    StructField("city", StringType(), True),
    StructField("pincode", StringType(), True),
    StructField("city_type", StringType(), True),
])

# Unnormalized student-course data for normalization demo
student_course_data = [
    (101, "Rahul", "Math,Physics", "Sharma,Mehta", "sharma@abc.com,mehta@abc.com"),
    (102, "Priya", "Chemistry", "Iyer", "iyer@abc.com"),
]

student_course_schema = StructType([
    StructField("student_id", IntegerType(), True),
    StructField("student_name", StringType(), True),
    StructField("courses", StringType(), True),
    StructField("instructors", StringType(), True),
    StructField("instructor_emails", StringType(), True),
])

customers_df = spark.createDataFrame(customer_data, customer_schema)
products_df = spark.createDataFrame(product_data, product_schema)
orders_df = spark.createDataFrame(order_data, order_schema)
external_catalog_df = spark.createDataFrame(external_catalog_data, external_catalog_schema)
geo_lookup_df = spark.createDataFrame(geo_lookup_data, geo_lookup_schema)
student_course_df = spark.createDataFrame(student_course_data, student_course_schema)

orders_df = orders_df.withColumn("order_date", to_date(col("order_date_raw"), "yyyy-MM-dd"))

print("\n================ CUSTOMERS ================\n")
customers_df.show(truncate=False)

print("\n================ PRODUCTS ================\n")
products_df.show(truncate=False)

print("\n================ ORDERS ================\n")
orders_df.show(truncate=False)

# ============================================================
# SECTION 6: CROSS REFERENCING
# ============================================================
print("\n========================================================")
print("SECTION 6: CROSS REFERENCING")
print("Exercises:")
print("1. Check whether customer_id in orders exists in customer master")
print("2. Check whether product_id in orders exists in product master")
print("3. Compare internal product master with external catalog")
print("========================================================\n")

# 6.1 Orders referencing invalid customers
invalid_customer_ref_df = orders_df.alias("o").join(
    customers_df.alias("c"),
    col("o.customer_id") == col("c.customer_id"),
    "left"
).filter(col("c.customer_id").isNull()) \
 .select(
     col("o.order_id"),
     col("o.customer_id"),
     col("o.product_id"),
     lit("Invalid customer reference").alias("issue")
 )

print("Orders with invalid customer reference:")
invalid_customer_ref_df.show(truncate=False)

# 6.2 Orders referencing invalid products
invalid_product_ref_df = orders_df.alias("o").join(
    products_df.alias("p"),
    col("o.product_id") == col("p.product_id"),
    "left"
).filter(col("p.product_id").isNull()) \
 .select(
     col("o.order_id"),
     col("o.customer_id"),
     col("o.product_id"),
     lit("Invalid product reference").alias("issue")
 )

print("Orders with invalid product reference:")
invalid_product_ref_df.show(truncate=False)

# 6.3 Cross-check product master against external catalog
product_cross_check_df = products_df.alias("p").join(
    external_catalog_df.alias("e"),
    col("p.product_id") == col("e.product_id"),
    "inner"
).select(
    col("p.product_id"),
    col("p.product_name").alias("internal_product_name"),
    col("e.catalog_product_name").alias("external_product_name"),
    when(col("p.product_name") == col("e.catalog_product_name"), "MATCH")
    .otherwise("MISMATCH")
    .alias("name_check_status")
)

print("Internal vs External Product Name Comparison:")
product_cross_check_df.show(truncate=False)

# ============================================================
# SECTION 7: DATA QUALITY DASHBOARDS
# ============================================================
print("\n========================================================")
print("SECTION 7: DATA QUALITY DASHBOARDS")
print("Exercises:")
print("1. Build row-level quality flags")
print("2. Generate summary quality metrics")
print("3. Create a simple dashboard-style metrics table")
print("========================================================\n")

# Create row-level quality flags
orders_quality_df = orders_df \
    .withColumn("customer_ref_valid",
                when(col("customer_id").isin([101, 102, 103, 104]), "Y").otherwise("N")) \
    .withColumn("product_ref_valid",
                when(col("product_id").isin(["P101", "P102", "P103", "P104"]), "Y").otherwise("N")) \
    .withColumn("quantity_valid",
                when(col("quantity").isNotNull() & (col("quantity") > 0), "Y").otherwise("N")) \
    .withColumn("amount_valid",
                when(col("order_amount").isNotNull() & (col("order_amount") > 0), "Y").otherwise("N"))

print("Orders with quality flags:")
orders_quality_df.show(truncate=False)

# Dashboard summary metrics
dashboard_metrics_df = orders_quality_df.select(
    count("*").alias("total_orders"),
    spark_sum(when(col("customer_ref_valid") == "N", 1).otherwise(0)).alias("invalid_customer_refs"),
    spark_sum(when(col("product_ref_valid") == "N", 1).otherwise(0)).alias("invalid_product_refs"),
    spark_sum(when(col("quantity_valid") == "N", 1).otherwise(0)).alias("invalid_or_null_quantity"),
    spark_sum(when(col("amount_valid") == "N", 1).otherwise(0)).alias("invalid_or_null_amount"),
    spark_sum(when(col("order_amount").isNull(), 1).otherwise(0)).alias("null_amount_count"),
    spark_sum(when(col("quantity").isNull(), 1).otherwise(0)).alias("null_quantity_count")
)

print("Dashboard Metrics:")
dashboard_metrics_df.show(truncate=False)

# Simple KPI-style table
dashboard_kpi_data = [
    ("Total Orders", orders_quality_df.count()),
    ("Orders with Invalid Customer Ref", orders_quality_df.filter(col("customer_ref_valid") == "N").count()),
    ("Orders with Invalid Product Ref", orders_quality_df.filter(col("product_ref_valid") == "N").count()),
    ("Orders with Invalid Quantity", orders_quality_df.filter(col("quantity_valid") == "N").count()),
    ("Orders with Invalid Amount", orders_quality_df.filter(col("amount_valid") == "N").count()),
]

dashboard_kpi_schema = StructType([
    StructField("metric_name", StringType(), True),
    StructField("metric_value", IntegerType(), True),
])

dashboard_kpi_df = spark.createDataFrame(dashboard_kpi_data, dashboard_kpi_schema)

print("Simple Dashboard KPI Table:")
dashboard_kpi_df.show(truncate=False)

# ============================================================
# SECTION 8: NORMALIZATION
# ============================================================
print("\n========================================================")
print("SECTION 8: NORMALIZATION")
print("Exercises:")
print("1. Show unnormalized data")
print("2. Convert it into 1NF-style atomic rows")
print("3. Build normalized tables similar to 2NF/3NF")
print("========================================================\n")

print("Unnormalized Student-Course Data:")
student_course_df.show(truncate=False)

# 1NF-style atomic rows manually created from Python variables
enrollment_atomic_data = [
    (101, "Rahul", "Math", "Sharma", "sharma@abc.com"),
    (101, "Rahul", "Physics", "Mehta", "mehta@abc.com"),
    (102, "Priya", "Chemistry", "Iyer", "iyer@abc.com"),
]

enrollment_atomic_schema = StructType([
    StructField("student_id", IntegerType(), True),
    StructField("student_name", StringType(), True),
    StructField("course", StringType(), True),
    StructField("instructor", StringType(), True),
    StructField("instructor_email", StringType(), True),
])

atomic_df = spark.createDataFrame(enrollment_atomic_data, enrollment_atomic_schema)

print("1NF-style Atomic Data:")
atomic_df.show(truncate=False)

# 2NF / 3NF style normalized tables
students_norm_df = atomic_df.select("student_id", "student_name").dropDuplicates()
courses_norm_df = atomic_df.select("course", "instructor").dropDuplicates()
instructors_norm_df = atomic_df.select("instructor", "instructor_email").dropDuplicates()
enrollments_norm_df = atomic_df.select("student_id", "course").dropDuplicates()

print("Normalized Student Table:")
students_norm_df.show(truncate=False)

print("Normalized Course Table:")
courses_norm_df.show(truncate=False)

print("Normalized Instructor Table:")
instructors_norm_df.show(truncate=False)

print("Normalized Enrollment Table:")
enrollments_norm_df.show(truncate=False)

# ============================================================
# SECTION 9: DATA ENRICHMENT
# ============================================================
print("\n========================================================")
print("SECTION 9: DATA ENRICHMENT")
print("Exercises:")
print("1. Add pincode and city type using lookup join")
print("2. Derive full location field")
print("3. Join product details into orders")
print("4. Create high-value-order flag")
print("========================================================\n")

# Enrich customers with geo information
enriched_customers_df = customers_df.join(
    geo_lookup_df,
    on="city",
    how="left"
).withColumn(
    "full_location",
    concat_ws(", ", col("city"), col("state"), col("pincode"))
)

print("Enriched Customers:")
enriched_customers_df.show(truncate=False)

# Enrich orders with customer and product details
enriched_orders_df = orders_df.alias("o") \
    .join(customers_df.alias("c"), col("o.customer_id") == col("c.customer_id"), "left") \
    .join(products_df.alias("p"), col("o.product_id") == col("p.product_id"), "left") \
    .select(
        col("o.order_id"),
        col("o.customer_id"),
        col("c.customer_name"),
        col("c.city"),
        col("c.state"),
        col("o.product_id"),
        col("p.product_name"),
        col("p.category"),
        col("o.order_date"),
        col("o.quantity"),
        col("o.order_amount")
    ) \
    .withColumn(
        "high_value_order_flag",
        when(col("order_amount") >= 50000, "Y").otherwise("N")
    ) \
    .withColumn(
        "order_year",
        year(col("order_date"))
    ) \
    .withColumn(
        "order_month",
        month(col("order_date"))
    )

print("Enriched Orders:")
enriched_orders_df.show(truncate=False)

# ============================================================
# SECTION 10: DATA AGGREGATION
# ============================================================
print("\n========================================================")
print("SECTION 10: DATA AGGREGATION")
print("Exercises:")
print("1. Aggregate total sales by product")
print("2. Aggregate total sales by category")
print("3. Aggregate total sales by city")
print("4. Aggregate monthly sales")
print("5. Aggregate customer-wise order metrics")
print("========================================================\n")

# 10.1 Product-wise aggregation
product_sales_df = enriched_orders_df.groupBy("product_id", "product_name").agg(
    count("order_id").alias("total_orders"),
    spark_sum("quantity").alias("total_quantity"),
    spark_sum("order_amount").alias("total_sales")
).orderBy(col("total_sales").desc_nulls_last())

print("Product-wise Sales Summary:")
product_sales_df.show(truncate=False)

# 10.2 Category-wise aggregation
category_sales_df = enriched_orders_df.groupBy("category").agg(
    count("order_id").alias("order_count"),
    spark_sum("order_amount").alias("category_sales"),
    avg("order_amount").alias("avg_order_value")
).orderBy(col("category_sales").desc_nulls_last())

print("Category-wise Sales Summary:")
category_sales_df.show(truncate=False)

# 10.3 City-wise aggregation
city_sales_df = enriched_orders_df.groupBy("city", "state").agg(
    count("order_id").alias("order_count"),
    spark_sum("order_amount").alias("city_sales")
).orderBy(col("city_sales").desc_nulls_last())

print("City-wise Sales Summary:")
city_sales_df.show(truncate=False)

# 10.4 Monthly aggregation
monthly_sales_df = enriched_orders_df.groupBy("order_year", "order_month").agg(
    count("order_id").alias("order_count"),
    spark_sum("order_amount").alias("monthly_sales")
).orderBy("order_year", "order_month")

print("Monthly Sales Summary:")
monthly_sales_df.show(truncate=False)

# 10.5 Customer-wise aggregation
customer_summary_df = enriched_orders_df.groupBy("customer_id", "customer_name").agg(
    count("order_id").alias("total_orders"),
    spark_sum("order_amount").alias("total_spent"),
    avg("order_amount").alias("avg_order_value"),
    countDistinct("product_id").alias("distinct_products_bought")
).orderBy(col("total_spent").desc_nulls_last())

print("Customer-wise Order Summary:")
customer_summary_df.show(truncate=False)

# ------------------------------------------------------------
# OPTIONAL FINAL REPORT DATASET
# ------------------------------------------------------------
final_report_df = enriched_orders_df.groupBy(
    "state", "city", "category"
).agg(
    count("order_id").alias("order_count"),
    spark_sum("order_amount").alias("total_sales"),
    avg("order_amount").alias("avg_order_value")
).orderBy("state", "city", "category")

print("\n================ FINAL REPORT DATASET ================\n")
final_report_df.show(truncate=False)

# ------------------------------------------------------------
# STOP SESSION
# ------------------------------------------------------------
spark.stop()


##########################################################################
##########################################################################


# ============================================================
# PYSPARK HANDS-ON SCRIPT - PART 3
# Covers next 5 sections:
# 11. Enrichment and Aggregation Synergy
# 12. Data Quality Issues
# 13. Building a Culture of Data Quality
# 14. Handling Anomalies
# 15. Factors Contributing to Data Transformation
#
# DataFrames are created from Python variables only.
# ============================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, count, sum as spark_sum, avg,
    min as spark_min, max as spark_max, countDistinct,
    to_date, month, year, regexp_replace, trim, initcap, lower,
    current_timestamp, expr
)
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType,
    DoubleType
)

# ------------------------------------------------------------
# 0. SPARK SESSION
# ------------------------------------------------------------
spark = SparkSession.builder \
    .appName("ETL_Terminologies_HandsOn_Part3") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# ------------------------------------------------------------
# 1. SOURCE DATA FROM PYTHON VARIABLES
# ------------------------------------------------------------
sales_data = [
    (1, "C101", "P101", "nagpur", "2024-12-01", 2, 50000.0, "mobile", "web"),
    (2, "C102", "P102", "Pune", "2024-12-01", 1, 25000.0, "laptop", "store"),
    (3, "C103", "P103", "INDORE", "2024-12-02", 3, 21000.0, "table", "web"),
    (4, "C104", "P999", "Mumbai", "2024-12-02", 1, 9999999.0, "unknown", "web"),  # invalid product + outlier
    (5, None,   "P104", "Nagpur", "2024-12-03", None, 3000.0, "chair", "store"),   # missing customer + quantity
    (6, "C106", "P102", "Pune", "2024-12-03", 2, None, "Laptop", "web"),            # missing amount
    (7, "C107", "P101", "Bharat", "2024-12-04", 1, -100.0, "mobile", "app"),        # inconsistent city + negative amount
    (8, "C108", "P103", "Indore", "2024-12-04", 1, 7000.0, "Table", "app"),
]

sales_schema = StructType([
    StructField("sale_id", IntegerType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("location_raw", StringType(), True),
    StructField("sale_date_raw", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("amount", DoubleType(), True),
    StructField("product_name_raw", StringType(), True),
    StructField("channel", StringType(), True),
])

product_master_data = [
    ("P101", "Mobile", "Electronics", 25000.0),
    ("P102", "Laptop", "Electronics", 50000.0),
    ("P103", "Table", "Furniture", 7000.0),
    ("P104", "Chair", "Furniture", 3000.0),
]

product_master_schema = StructType([
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("base_price", DoubleType(), True),
])

geo_data = [
    ("Nagpur", "Maharashtra", "Tier-2"),
    ("Pune", "Maharashtra", "Metro"),
    ("Indore", "Madhya Pradesh", "Tier-2"),
    ("Mumbai", "Maharashtra", "Metro"),
    ("India", "National", "Country"),
]

geo_schema = StructType([
    StructField("city_standardized", StringType(), True),
    StructField("state", StringType(), True),
    StructField("region_type", StringType(), True),
])

sales_df = spark.createDataFrame(sales_data, sales_schema)
product_master_df = spark.createDataFrame(product_master_data, product_master_schema)
geo_df = spark.createDataFrame(geo_data, geo_schema)

print("\n================ RAW SALES DATA ================\n")
sales_df.show(truncate=False)

print("\n================ PRODUCT MASTER ================\n")
product_master_df.show(truncate=False)

print("\n================ GEO MASTER ================\n")
geo_df.show(truncate=False)

# ------------------------------------------------------------
# 2. BASIC PREPARATION
# ------------------------------------------------------------
prepared_sales_df = sales_df \
    .withColumn("sale_date", to_date(col("sale_date_raw"), "yyyy-MM-dd")) \
    .withColumn("location_clean",
        when(lower(trim(col("location_raw"))).isin("nagpur"), lit("Nagpur"))
        .when(lower(trim(col("location_raw"))).isin("pune"), lit("Pune"))
        .when(lower(trim(col("location_raw"))).isin("indore"), lit("Indore"))
        .when(lower(trim(col("location_raw"))).isin("mumbai"), lit("Mumbai"))
        .when(lower(trim(col("location_raw"))).isin("bharat", "india"), lit("India"))
        .otherwise(initcap(trim(col("location_raw"))))
    ) \
    .withColumn("product_name_clean", initcap(trim(col("product_name_raw")))) \
    .withColumn("sale_year", year(col("sale_date"))) \
    .withColumn("sale_month", month(col("sale_date")))

print("\n================ PREPARED SALES DATA ================\n")
prepared_sales_df.show(truncate=False)

# ============================================================
# SECTION 11: ENRICHMENT AND AGGREGATION SYNERGY
# ============================================================
print("\n========================================================")
print("SECTION 11: ENRICHMENT AND AGGREGATION SYNERGY")
print("Exercises:")
print("1. Enrich sales with product and geography data")
print("2. Create derived fields after enrichment")
print("3. Aggregate sales using enriched columns")
print("========================================================\n")

enriched_sales_df = prepared_sales_df.alias("s") \
    .join(product_master_df.alias("p"), col("s.product_id") == col("p.product_id"), "left") \
    .join(geo_df.alias("g"), col("s.location_clean") == col("g.city_standardized"), "left") \
    .select(
        col("s.sale_id"),
        col("s.customer_id"),
        col("s.product_id"),
        col("p.product_name").alias("master_product_name"),
        col("p.category"),
        col("p.base_price"),
        col("s.location_clean").alias("city"),
        col("g.state"),
        col("g.region_type"),
        col("s.sale_date"),
        col("s.sale_year"),
        col("s.sale_month"),
        col("s.quantity"),
        col("s.amount"),
        col("s.channel")
    ) \
    .withColumn(
        "high_value_flag",
        when(col("amount") >= 50000, "Y").otherwise("N")
    ) \
    .withColumn(
        "expected_amount",
        col("quantity") * col("base_price")
    )

print("Enriched Sales Data:")
enriched_sales_df.show(truncate=False)

# Aggregation after enrichment
sales_by_region_df = enriched_sales_df.groupBy("state", "region_type", "category").agg(
    count("sale_id").alias("transaction_count"),
    spark_sum("quantity").alias("total_qty"),
    spark_sum("amount").alias("total_sales"),
    avg("amount").alias("avg_sales")
).orderBy("state", "category")

print("Aggregation on Enriched Data (state/region/category):")
sales_by_region_df.show(truncate=False)

high_value_by_channel_df = enriched_sales_df.groupBy("channel", "high_value_flag").agg(
    count("sale_id").alias("txns"),
    spark_sum("amount").alias("sales_amount")
).orderBy("channel", "high_value_flag")

print("High Value Sales by Channel:")
high_value_by_channel_df.show(truncate=False)

# ============================================================
# SECTION 12: DATA QUALITY ISSUES
# ============================================================
print("\n========================================================")
print("SECTION 12: DATA QUALITY ISSUES")
print("Exercises:")
print("1. Detect missing values")
print("2. Detect inconsistent values")
print("3. Detect invalid references")
print("4. Detect outliers and suspicious data")
print("========================================================\n")

# Missing values
print("Null count profile:")
prepared_sales_df.select([
    spark_sum(col(c).isNull().cast("int")).alias(f"{c}_nulls")
    for c in prepared_sales_df.columns
]).show(truncate=False)

# Inconsistent values before standardization
print("Distinct raw location values:")
sales_df.select("location_raw").distinct().show(truncate=False)

print("Distinct raw product name values:")
sales_df.select("product_name_raw").distinct().show(truncate=False)

# Invalid product references
invalid_product_ref_df = prepared_sales_df.alias("s").join(
    product_master_df.alias("p"),
    col("s.product_id") == col("p.product_id"),
    "left"
).filter(col("p.product_id").isNull()) \
 .select(
     col("s.sale_id"),
     col("s.product_id"),
     lit("Invalid product reference").alias("issue")
 )

print("Invalid Product References:")
invalid_product_ref_df.show(truncate=False)

# Suspicious values
quality_issue_df = prepared_sales_df \
    .withColumn("missing_customer_flag", when(col("customer_id").isNull(), "Y").otherwise("N")) \
    .withColumn("missing_quantity_flag", when(col("quantity").isNull(), "Y").otherwise("N")) \
    .withColumn("missing_amount_flag", when(col("amount").isNull(), "Y").otherwise("N")) \
    .withColumn("negative_amount_flag", when(col("amount") < 0, "Y").otherwise("N")) \
    .withColumn("amount_outlier_flag", when(col("amount") > 500000, "Y").otherwise("N"))

print("Data Quality Issue Flags:")
quality_issue_df.select(
    "sale_id", "customer_id", "product_id", "amount", "quantity",
    "missing_customer_flag", "missing_quantity_flag", "missing_amount_flag",
    "negative_amount_flag", "amount_outlier_flag"
).show(truncate=False)

# ============================================================
# SECTION 13: BUILDING A CULTURE OF DATA QUALITY
# ============================================================
print("\n========================================================")
print("SECTION 13: BUILDING A CULTURE OF DATA QUALITY")
print("Exercises:")
print("1. Build reusable quality checks")
print("2. Generate a quality metrics table")
print("3. Generate pass/fail monitoring output")
print("========================================================\n")

quality_metrics_df = quality_issue_df.select(
    count("*").alias("total_records"),
    spark_sum(when(col("missing_customer_flag") == "Y", 1).otherwise(0)).alias("missing_customer_count"),
    spark_sum(when(col("missing_quantity_flag") == "Y", 1).otherwise(0)).alias("missing_quantity_count"),
    spark_sum(when(col("missing_amount_flag") == "Y", 1).otherwise(0)).alias("missing_amount_count"),
    spark_sum(when(col("negative_amount_flag") == "Y", 1).otherwise(0)).alias("negative_amount_count"),
    spark_sum(when(col("amount_outlier_flag") == "Y", 1).otherwise(0)).alias("amount_outlier_count"),
)

print("Quality Metrics Table:")
quality_metrics_df.show(truncate=False)

# Monitoring-style output
monitoring_df = quality_issue_df.select(
    lit("sales_pipeline").alias("pipeline_name"),
    current_timestamp().alias("check_ts"),
    count("*").alias("total_records"),
    spark_sum(when(col("missing_customer_flag") == "Y", 1).otherwise(0)).alias("missing_customer_count"),
    spark_sum(when(col("missing_amount_flag") == "Y", 1).otherwise(0)).alias("missing_amount_count"),
    spark_sum(when(col("amount_outlier_flag") == "Y", 1).otherwise(0)).alias("outlier_count")
).withColumn(
    "pipeline_status",
    when(
        (col("missing_customer_count") > 0) |
        (col("missing_amount_count") > 0) |
        (col("outlier_count") > 0),
        "ALERT"
    ).otherwise("HEALTHY")
)

print("Monitoring Output:")
monitoring_df.show(truncate=False)

# Rule-based exception dataset
exceptions_df = quality_issue_df.filter(
    (col("missing_customer_flag") == "Y") |
    (col("missing_quantity_flag") == "Y") |
    (col("missing_amount_flag") == "Y") |
    (col("negative_amount_flag") == "Y") |
    (col("amount_outlier_flag") == "Y")
)

print("Exception Records for Review:")
exceptions_df.show(truncate=False)

# ============================================================
# SECTION 14: HANDLING ANOMALIES
# ============================================================
print("\n========================================================")
print("SECTION 14: HANDLING ANOMALIES")
print("Exercises:")
print("1. Investigate anomalies")
print("2. Apply capping (winsorization-style simplified)")
print("3. Use robust metrics such as median-like logic via approx percentile")
print("========================================================\n")

print("Investigating suspicious high/low amounts:")
prepared_sales_df.filter(
    (col("amount") < 0) | (col("amount") > 500000)
).show(truncate=False)

# Simplified winsorization/capping
anomaly_handled_df = prepared_sales_df \
    .withColumn(
        "amount_capped",
        when(col("amount") < 0, 0.0)
        .when(col("amount") > 100000, 100000.0)
        .otherwise(col("amount"))
    )

print("After Capping Extreme Amounts:")
anomaly_handled_df.select(
    "sale_id", "amount", "amount_capped"
).show(truncate=False)

# Approx percentile based robust thresholds
percentile_df = prepared_sales_df.selectExpr(
    "percentile_approx(amount, 0.25) as q1",
    "percentile_approx(amount, 0.5) as median",
    "percentile_approx(amount, 0.75) as q3"
)

print("Approx Percentiles for Amount:")
percentile_df.show(truncate=False)

# Use IQR logic
iqr_values = percentile_df.collect()[0]
q1 = iqr_values["q1"]
q3 = iqr_values["q3"]
iqr = q3 - q1 if q1 is not None and q3 is not None else None

if iqr is not None:
    lower_bound = q1 - 1.5 * iqr
    upper_bound = q3 + 1.5 * iqr

    print(f"IQR Lower Bound: {lower_bound}")
    print(f"IQR Upper Bound: {upper_bound}")

    iqr_outliers_df = prepared_sales_df.filter(
        (col("amount") < lit(lower_bound)) | (col("amount") > lit(upper_bound))
    )

    print("Outliers Detected Using IQR:")
    iqr_outliers_df.show(truncate=False)

# ============================================================
# SECTION 15: FACTORS CONTRIBUTING TO DATA TRANSFORMATION
# ============================================================
print("\n========================================================")
print("SECTION 15: FACTORS CONTRIBUTING TO DATA TRANSFORMATION")
print("Exercises:")
print("1. Handle heterogeneous source formats")
print("2. Simulate schema/meaning inconsistency")
print("3. Show transformation complexity in a multi-step pipeline")
print("========================================================\n")

# Heterogeneous source simulation: sales from another source with different format/meaning
api_sales_data = [
    ("A001", "C201", "P101", "2024/12/05", "Nagpur", "1", "25000"),
    ("A002", "C202", "P102", "2024/12/05", "Pune", "2", "100000"),
    ("A003", "C203", "P103", "2024/12/06", "Mumbai", "1", "7000"),
]

api_sales_schema = StructType([
    StructField("api_sale_id", StringType(), True),
    StructField("api_customer_id", StringType(), True),
    StructField("api_product_id", StringType(), True),
    StructField("api_date", StringType(), True),
    StructField("api_city", StringType(), True),
    StructField("api_qty", StringType(), True),
    StructField("api_amount", StringType(), True),
])

api_sales_df = spark.createDataFrame(api_sales_data, api_sales_schema)

print("Raw API Sales Data:")
api_sales_df.show(truncate=False)

# Standardize second source to same shape as first source
api_sales_standardized_df = api_sales_df \
    .withColumnRenamed("api_sale_id", "sale_id_source2") \
    .withColumnRenamed("api_customer_id", "customer_id") \
    .withColumnRenamed("api_product_id", "product_id") \
    .withColumnRenamed("api_city", "location_raw") \
    .withColumn("sale_date", to_date(col("api_date"), "yyyy/MM/dd")) \
    .withColumn("quantity", col("api_qty").cast("int")) \
    .withColumn("amount", col("api_amount").cast("double")) \
    .select(
        "sale_id_source2", "customer_id", "product_id", "location_raw",
        "sale_date", "quantity", "amount"
    )

print("Standardized API Sales Data:")
api_sales_standardized_df.show(truncate=False)

# Multi-step transformation complexity demo
complex_pipeline_df = api_sales_standardized_df \
    .withColumn(
        "location_clean",
        when(lower(trim(col("location_raw"))) == "nagpur", "Nagpur")
        .when(lower(trim(col("location_raw"))) == "pune", "Pune")
        .when(lower(trim(col("location_raw"))) == "mumbai", "Mumbai")
        .otherwise(initcap(trim(col("location_raw"))))
    ) \
    .join(product_master_df, on="product_id", how="left") \
    .join(geo_df, col("location_clean") == col("city_standardized"), "left") \
    .withColumn("expected_amount", col("quantity") * col("base_price")) \
    .withColumn(
        "amount_match_flag",
        when(col("amount") == col("expected_amount"), "Y").otherwise("N")
    )

print("Complex Transformation Pipeline Result:")
complex_pipeline_df.show(truncate=False)

# Show why transformation is needed across evolving and mixed sources
transformation_challenges_summary_df = complex_pipeline_df.select(
    count("*").alias("total_source2_records"),
    spark_sum(when(col("category").isNull(), 1).otherwise(0)).alias("missing_product_mapping"),
    spark_sum(when(col("state").isNull(), 1).otherwise(0)).alias("missing_geo_mapping"),
    spark_sum(when(col("amount_match_flag") == "N", 1).otherwise(0)).alias("amount_mismatch_count")
)

print("Transformation Challenge Summary:")
transformation_challenges_summary_df.show(truncate=False)

# ------------------------------------------------------------
# OPTIONAL FINAL DATASET
# ------------------------------------------------------------
final_output_df = enriched_sales_df.select(
    "sale_id", "customer_id", "product_id", "master_product_name",
    "category", "city", "state", "region_type", "sale_date",
    "quantity", "amount", "high_value_flag"
)

print("\n================ FINAL OUTPUT DATASET ================\n")
final_output_df.show(truncate=False)

# ------------------------------------------------------------
# STOP SESSION
# ------------------------------------------------------------
spark.stop()


##########################################################################
##########################################################################


# ============================================================
# PYSPARK HANDS-ON SCRIPT - PART 4
# Covers last 2 sections:
# 16. Strategies for Managing Complexity
# 17. External Data Integration
#
# DataFrames are created from Python variables only.
# ============================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, count, sum as spark_sum, avg,
    trim, lower, initcap, to_date, current_timestamp,
    concat_ws
)
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, DoubleType
)

# ------------------------------------------------------------
# 0. SPARK SESSION
# ------------------------------------------------------------
spark = SparkSession.builder \
    .appName("ETL_Terminologies_HandsOn_Part4") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# ------------------------------------------------------------
# 1. SOURCE DATA FROM PYTHON VARIABLES
# ------------------------------------------------------------
orders_data = [
    (1001, "C101", "P101", "2024-12-01", "nagpur", 2, 50000.0),
    (1002, "C102", "P102", "2024-12-01", "Pune", 1, 25000.0),
    (1003, "C103", "P103", "2024-12-02", "INDORE", 3, 21000.0),
    (1004, "C104", "P104", "2024-12-02", "Mumbai", 1, 3000.0),
    (1005, "C105", "P101", "2024-12-03", "Bharat", 1, 25000.0),   # inconsistent city
    (1006, "C106", "P999", "2024-12-03", "Pune", 2, 100000.0),    # invalid product
    (1007, "C107", "P102", "2024-12-04", None, 1, None),          # missing city, amount
]

orders_schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("order_date_raw", StringType(), True),
    StructField("city_raw", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("amount", DoubleType(), True),
])

product_master_data = [
    ("P101", "Mobile", "Electronics", 25000.0),
    ("P102", "Laptop", "Electronics", 50000.0),
    ("P103", "Table", "Furniture", 7000.0),
    ("P104", "Chair", "Furniture", 3000.0),
]

product_master_schema = StructType([
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("base_price", DoubleType(), True),
])

# External data source 1: weather data
weather_data = [
    ("Nagpur", "2024-12-01", "Sunny", 32.0),
    ("Pune", "2024-12-01", "Cloudy", 28.0),
    ("Indore", "2024-12-02", "Rainy", 24.0),
    ("Mumbai", "2024-12-02", "Humid", 31.0),
    ("Pune", "2024-12-03", "Sunny", 29.0),
]

weather_schema = StructType([
    StructField("city", StringType(), True),
    StructField("weather_date_raw", StringType(), True),
    StructField("weather_condition", StringType(), True),
    StructField("temperature_c", DoubleType(), True),
])

# External data source 2: market trend / campaign score
market_data = [
    ("Electronics", "2024-12", 1.20, "High Demand"),
    ("Furniture", "2024-12", 0.85, "Low Demand"),
]

market_schema = StructType([
    StructField("category", StringType(), True),
    StructField("year_month", StringType(), True),
    StructField("demand_index", DoubleType(), True),
    StructField("market_signal", StringType(), True),
])

orders_df = spark.createDataFrame(orders_data, orders_schema)
product_master_df = spark.createDataFrame(product_master_data, product_master_schema)
weather_df = spark.createDataFrame(weather_data, weather_schema)
market_df = spark.createDataFrame(market_data, market_schema)

print("\n================ RAW ORDERS DATA ================\n")
orders_df.show(truncate=False)

print("\n================ PRODUCT MASTER ================\n")
product_master_df.show(truncate=False)

print("\n================ WEATHER DATA ================\n")
weather_df.show(truncate=False)

print("\n================ MARKET DATA ================\n")
market_df.show(truncate=False)

# ------------------------------------------------------------
# 2. MODULAR PIPELINE STEPS
#    (Used to demonstrate strategy for managing complexity)
# ------------------------------------------------------------

def standardize_orders(df):
    """
    Step 1: Standardize source data
    """
    return df \
        .withColumn("order_date", to_date(col("order_date_raw"), "yyyy-MM-dd")) \
        .withColumn(
            "city_standardized",
            when(lower(trim(col("city_raw"))).isin("nagpur"), lit("Nagpur"))
            .when(lower(trim(col("city_raw"))).isin("pune"), lit("Pune"))
            .when(lower(trim(col("city_raw"))).isin("indore"), lit("Indore"))
            .when(lower(trim(col("city_raw"))).isin("mumbai"), lit("Mumbai"))
            .when(lower(trim(col("city_raw"))).isin("bharat", "india"), lit("India"))
            .otherwise(initcap(trim(col("city_raw"))))
        )

def validate_orders(df):
    """
    Step 2: Apply validation / quality checks
    """
    return df \
        .withColumn("city_valid", when(col("city_standardized").isNotNull(), "Y").otherwise("N")) \
        .withColumn("amount_valid", when(col("amount").isNotNull() & (col("amount") > 0), "Y").otherwise("N")) \
        .withColumn("quantity_valid", when(col("quantity").isNotNull() & (col("quantity") > 0), "Y").otherwise("N"))

def enrich_with_product(df, product_df):
    """
    Step 3: Enrich with internal master data
    """
    return df.join(product_df, on="product_id", how="left") \
        .withColumn("product_ref_valid", when(col("product_name").isNotNull(), "Y").otherwise("N")) \
        .withColumn("expected_amount", col("quantity") * col("base_price"))

def build_quality_monitor(df):
    """
    Step 4: Build monitoring output
    """
    return df.select(
        lit("orders_pipeline").alias("pipeline_name"),
        current_timestamp().alias("check_ts"),
        count("*").alias("total_records"),
        spark_sum(when(col("city_valid") == "N", 1).otherwise(0)).alias("invalid_city_count"),
        spark_sum(when(col("amount_valid") == "N", 1).otherwise(0)).alias("invalid_amount_count"),
        spark_sum(when(col("product_ref_valid") == "N", 1).otherwise(0)).alias("invalid_product_ref_count")
    ).withColumn(
        "pipeline_status",
        when(
            (col("invalid_city_count") > 0) |
            (col("invalid_amount_count") > 0) |
            (col("invalid_product_ref_count") > 0),
            "ALERT"
        ).otherwise("HEALTHY")
    )

# ============================================================
# SECTION 16: STRATEGIES FOR MANAGING COMPLEXITY
# ============================================================
print("\n========================================================")
print("SECTION 16: STRATEGIES FOR MANAGING COMPLEXITY")
print("Exercises:")
print("1. Break pipeline into modular reusable steps")
print("2. Apply common data standards")
print("3. Build monitoring output")
print("4. Separate good records and exception records")
print("========================================================\n")

# Step-by-step modular flow
standardized_orders_df = standardize_orders(orders_df)
print("Step 1 - Standardized Orders:")
standardized_orders_df.show(truncate=False)

validated_orders_df = validate_orders(standardized_orders_df)
print("Step 2 - Validated Orders:")
validated_orders_df.show(truncate=False)

enriched_orders_df = enrich_with_product(validated_orders_df, product_master_df)
print("Step 3 - Enriched with Product Master:")
enriched_orders_df.show(truncate=False)

monitor_df = build_quality_monitor(enriched_orders_df)
print("Step 4 - Monitoring Output:")
monitor_df.show(truncate=False)

# Good records and exception records
good_orders_df = enriched_orders_df.filter(
    (col("city_valid") == "Y") &
    (col("amount_valid") == "Y") &
    (col("quantity_valid") == "Y") &
    (col("product_ref_valid") == "Y")
)

exception_orders_df = enriched_orders_df.filter(
    (col("city_valid") == "N") |
    (col("amount_valid") == "N") |
    (col("quantity_valid") == "N") |
    (col("product_ref_valid") == "N")
)

print("Good Records:")
good_orders_df.show(truncate=False)

print("Exception Records:")
exception_orders_df.show(truncate=False)

# Example of standardization summary
standards_summary_df = enriched_orders_df.select(
    count("*").alias("total_records"),
    spark_sum(when(col("city_standardized") == "India", 1).otherwise(0)).alias("records_standardized_to_india"),
    spark_sum(when(col("product_ref_valid") == "N", 1).otherwise(0)).alias("records_with_invalid_product"),
    spark_sum(when(col("amount_valid") == "N", 1).otherwise(0)).alias("records_with_invalid_amount")
)

print("Standards Summary:")
standards_summary_df.show(truncate=False)

# ============================================================
# SECTION 17: EXTERNAL DATA INTEGRATION
# ============================================================
print("\n========================================================")
print("SECTION 17: EXTERNAL DATA INTEGRATION")
print("Exercises:")
print("1. Integrate weather data with orders")
print("2. Integrate market trend data with orders")
print("3. Check external join coverage")
print("4. Build integrated business report")
print("========================================================\n")

# Prepare external data
weather_prepared_df = weather_df \
    .withColumn("weather_date", to_date(col("weather_date_raw"), "yyyy-MM-dd"))

market_prepared_df = market_df

# Add year_month to orders for market join
orders_for_external_df = good_orders_df \
    .withColumn(
        "year_month",
        concat_ws(
            "-",
            col("order_date").cast("string").substr(1, 4),
            col("order_date").cast("string").substr(6, 2)
        )
    )

# Integrate weather data
orders_with_weather_df = orders_for_external_df.alias("o").join(
    weather_prepared_df.alias("w"),
    (col("o.city_standardized") == col("w.city")) &
    (col("o.order_date") == col("w.weather_date")),
    "left"
).select(
    col("o.order_id"),
    col("o.customer_id"),
    col("o.product_id"),
    col("o.product_name"),
    col("o.category"),
    col("o.city_standardized"),
    col("o.order_date"),
    col("o.quantity"),
    col("o.amount"),
    col("w.weather_condition"),
    col("w.temperature_c")
)

print("Orders Enriched with Weather Data:")
orders_with_weather_df.show(truncate=False)

# Integrate market trend data
orders_with_external_df = orders_for_external_df.alias("o").join(
    market_prepared_df.alias("m"),
    (col("o.category") == col("m.category")) &
    (col("o.year_month") == col("m.year_month")),
    "left"
).select(
    col("o.order_id"),
    col("o.customer_id"),
    col("o.product_id"),
    col("o.product_name"),
    col("o.category"),
    col("o.city_standardized"),
    col("o.order_date"),
    col("o.quantity"),
    col("o.amount"),
    col("o.year_month"),
    col("m.demand_index"),
    col("m.market_signal")
)

print("Orders Enriched with Market Trend Data:")
orders_with_external_df.show(truncate=False)

# Combine both external enrichments
final_external_integrated_df = orders_for_external_df.alias("o") \
    .join(
        weather_prepared_df.alias("w"),
        (col("o.city_standardized") == col("w.city")) &
        (col("o.order_date") == col("w.weather_date")),
        "left"
    ) \
    .join(
        market_prepared_df.alias("m"),
        (col("o.category") == col("m.category")) &
        (col("o.year_month") == col("m.year_month")),
        "left"
    ) \
    .select(
        col("o.order_id"),
        col("o.customer_id"),
        col("o.product_id"),
        col("o.product_name"),
        col("o.category"),
        col("o.city_standardized").alias("city"),
        col("o.order_date"),
        col("o.quantity"),
        col("o.amount"),
        col("w.weather_condition"),
        col("w.temperature_c"),
        col("m.demand_index"),
        col("m.market_signal")
    ) \
    .withColumn(
        "external_data_coverage_flag",
        when(
            col("weather_condition").isNotNull() & col("market_signal").isNotNull(),
            "Y"
        ).otherwise("N")
    )

print("Final External Integrated Dataset:")
final_external_integrated_df.show(truncate=False)

# Coverage / monitoring for external integration
external_coverage_df = final_external_integrated_df.select(
    count("*").alias("total_records"),
    spark_sum(when(col("weather_condition").isNull(), 1).otherwise(0)).alias("missing_weather_records"),
    spark_sum(when(col("market_signal").isNull(), 1).otherwise(0)).alias("missing_market_records"),
    spark_sum(when(col("external_data_coverage_flag") == "Y", 1).otherwise(0)).alias("fully_enriched_records")
)

print("External Integration Coverage Summary:")
external_coverage_df.show(truncate=False)

# Business aggregation after external integration
business_report_df = final_external_integrated_df.groupBy(
    "category", "city", "weather_condition", "market_signal"
).agg(
    count("order_id").alias("order_count"),
    spark_sum("quantity").alias("total_quantity"),
    spark_sum("amount").alias("total_sales"),
    avg("amount").alias("avg_sales")
).orderBy("category", "city")

print("Business Report Using External Data:")
business_report_df.show(truncate=False)

# Example exception handling for missing external data
external_exception_df = final_external_integrated_df.filter(
    col("external_data_coverage_flag") == "N"
)

print("Records with Missing External Enrichment:")
external_exception_df.show(truncate=False)

# ------------------------------------------------------------
# OPTIONAL FINAL DATASET
# ------------------------------------------------------------
final_output_df = final_external_integrated_df.select(
    "order_id",
    "customer_id",
    "product_id",
    "product_name",
    "category",
    "city",
    "order_date",
    "quantity",
    "amount",
    "weather_condition",
    "temperature_c",
    "demand_index",
    "market_signal",
    "external_data_coverage_flag"
)

print("\n================ FINAL OUTPUT DATASET ================\n")
final_output_df.show(truncate=False)

# ------------------------------------------------------------
# STOP SESSION
# ------------------------------------------------------------
spark.stop()