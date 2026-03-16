# ============================================================
# DQ003 - Required column existence check
# Schema Definition Check Template
# ============================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.getOrCreate()

expected_columns_by_table = {'bronze_customer': ['customer_id', 'first_name', 'last_name', 'email', 'phone_number', 'status_code', '_ingest_ts', 'bronze_load_date'], 'bronze_orders': ['order_id', 'customer_id', 'order_date', 'order_status_code', '_ingest_ts', 'bronze_load_date'], 'bronze_order_item': ['order_item_id', 'order_id', 'product_variant_id', 'quantity', 'line_total_amount', '_ingest_ts', 'bronze_load_date'], 'bronze_product_catalog_feed': ['catalog_record_id', 'product_id', 'product_variant_id', 'seller_id', '_source_file', '_rescued_data', '_ingest_ts'], 'bronze_customer_review_feed': ['review_id', 'order_id', 'customer_id', 'product_id', 'rating', '_source_file', '_rescued_data', '_ingest_ts'], 'bronze_order_event_log': ['event_id', 'order_id', 'event_type', 'event_timestamp', '_source_file', '_rescued_data', '_ingest_ts']}

for table_name, expected_columns in expected_columns_by_table.items():
    print("=" * 80)
    print(f"PROCESSING {table_name}")
    print("=" * 80)

    # --------------------------------------------------------
    # Practice mode sample DataFrame
    # --------------------------------------------------------
    if table_name == "bronze_customer":
        sample_data = [("C101", "Amit", "Patil", "amit@gmail.com", "9999999999", "ACTIVE", "2026-03-10 09:00:00", "2026-03-10")]
        df = spark.createDataFrame(
            sample_data,
            "customer_id string, first_name string, last_name string, email string, phone_number string, status_code string, _ingest_ts string, bronze_load_date string"
        )
    elif table_name == "bronze_orders":
        sample_data = [("O1001", "C101", "2026-03-10 10:00:00", "PLACED", "2026-03-10 10:10:00", "2026-03-10")]
        df = spark.createDataFrame(
            sample_data,
            "order_id string, customer_id string, order_date string, order_status_code string, _ingest_ts string, bronze_load_date string"
        )
    elif table_name == "bronze_order_item":
        sample_data = [("OI101", "O1001", "PV101", 2, 215.0, "2026-03-10 10:10:00", "2026-03-10")]
        df = spark.createDataFrame(
            sample_data,
            "order_item_id string, order_id string, product_variant_id string, quantity int, line_total_amount double, _ingest_ts string, bronze_load_date string"
        )
    elif table_name == "bronze_product_catalog_feed":
        sample_data = [("CR1", "P101", "PV101", "S001", "f1.json", None, "2026-03-10 09:00:00")]
        df = spark.createDataFrame(
            sample_data,
            "catalog_record_id string, product_id string, product_variant_id string, seller_id string, _source_file string, _rescued_data string, _ingest_ts string"
        )
    elif table_name == "bronze_customer_review_feed":
        sample_data = [("R1", "O1", "C1", "P1", 5, "f2.json", None, "2026-03-10 10:00:00")]
        df = spark.createDataFrame(
            sample_data,
            "review_id string, order_id string, customer_id string, product_id string, rating int, _source_file string, _rescued_data string, _ingest_ts string"
        )
    elif table_name == "bronze_order_event_log":
        sample_data = [("E1", "O1", "ORDER", "2026-03-10 11:00:00", "f3.json", None, "2026-03-10 11:05:00")]
        df = spark.createDataFrame(
            sample_data,
            "event_id string, order_id string, event_type string, event_timestamp string, _source_file string, _rescued_data string, _ingest_ts string"
        )
    else:
        sample_data = [("X1", "2026-03-10 09:00:00")]
        df = spark.createDataFrame(sample_data, "id string, _ingest_ts string")

    # Uncomment below for production
    # df = spark.table(f"ecomsphere.bronze.{table_name}")

    actual_columns = set(df.columns)
    expected_columns_set = set(expected_columns)

    missing_columns = sorted(list(expected_columns_set - actual_columns))
    extra_columns = sorted(list(actual_columns - expected_columns_set))

    result_data = [(
        table_name,
        expected_columns,
        sorted(list(actual_columns)),
        missing_columns,
        extra_columns,
        "FAILED" if missing_columns else "PASSED"
    )]

    result_df = spark.createDataFrame(
        result_data,
        "table_name string, expected_columns array<string>, actual_columns array<string>, missing_columns array<string>, extra_columns array<string>, status string"
    )

    result_df.show(truncate=False)