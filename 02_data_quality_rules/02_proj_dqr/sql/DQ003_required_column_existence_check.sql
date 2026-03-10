-- ============================================================
-- DQ003 - Required column existence check
-- Schema Definition Check Template
-- ============================================================

-- ------------------------------------------------------------
-- TABLE: bronze_customer
-- Expected columns:
-- ['customer_id', 'first_name', 'last_name', 'email', 'phone_number', 'status_code', '_ingest_ts', 'bronze_load_date']
-- ------------------------------------------------------------

-- In Databricks production you would use:
-- DESCRIBE ecomsphere.bronze.bronze_customer;

-- This starter template documents expected schema.
SELECT
    'bronze_customer' AS table_name,
    'customer_id, first_name, last_name, email, phone_number, status_code, _ingest_ts, bronze_load_date' AS expected_columns_csv;
-- ------------------------------------------------------------
-- TABLE: bronze_orders
-- Expected columns:
-- ['order_id', 'customer_id', 'order_date', 'order_status_code', '_ingest_ts', 'bronze_load_date']
-- ------------------------------------------------------------

-- In Databricks production you would use:
-- DESCRIBE ecomsphere.bronze.bronze_orders;

-- This starter template documents expected schema.
SELECT
    'bronze_orders' AS table_name,
    'order_id, customer_id, order_date, order_status_code, _ingest_ts, bronze_load_date' AS expected_columns_csv;
-- ------------------------------------------------------------
-- TABLE: bronze_order_item
-- Expected columns:
-- ['order_item_id', 'order_id', 'product_variant_id', 'quantity', 'line_total_amount', '_ingest_ts', 'bronze_load_date']
-- ------------------------------------------------------------

-- In Databricks production you would use:
-- DESCRIBE ecomsphere.bronze.bronze_order_item;

-- This starter template documents expected schema.
SELECT
    'bronze_order_item' AS table_name,
    'order_item_id, order_id, product_variant_id, quantity, line_total_amount, _ingest_ts, bronze_load_date' AS expected_columns_csv;
-- ------------------------------------------------------------
-- TABLE: bronze_product_catalog_feed
-- Expected columns:
-- ['catalog_record_id', 'product_id', 'product_variant_id', 'seller_id', '_source_file', '_rescued_data', '_ingest_ts']
-- ------------------------------------------------------------

-- In Databricks production you would use:
-- DESCRIBE ecomsphere.bronze.bronze_product_catalog_feed;

-- This starter template documents expected schema.
SELECT
    'bronze_product_catalog_feed' AS table_name,
    'catalog_record_id, product_id, product_variant_id, seller_id, _source_file, _rescued_data, _ingest_ts' AS expected_columns_csv;
-- ------------------------------------------------------------
-- TABLE: bronze_customer_review_feed
-- Expected columns:
-- ['review_id', 'order_id', 'customer_id', 'product_id', 'rating', '_source_file', '_rescued_data', '_ingest_ts']
-- ------------------------------------------------------------

-- In Databricks production you would use:
-- DESCRIBE ecomsphere.bronze.bronze_customer_review_feed;

-- This starter template documents expected schema.
SELECT
    'bronze_customer_review_feed' AS table_name,
    'review_id, order_id, customer_id, product_id, rating, _source_file, _rescued_data, _ingest_ts' AS expected_columns_csv;
-- ------------------------------------------------------------
-- TABLE: bronze_order_event_log
-- Expected columns:
-- ['event_id', 'order_id', 'event_type', 'event_timestamp', '_source_file', '_rescued_data', '_ingest_ts']
-- ------------------------------------------------------------

-- In Databricks production you would use:
-- DESCRIBE ecomsphere.bronze.bronze_order_event_log;

-- This starter template documents expected schema.
SELECT
    'bronze_order_event_log' AS table_name,
    'event_id, order_id, event_type, event_timestamp, _source_file, _rescued_data, _ingest_ts' AS expected_columns_csv;
