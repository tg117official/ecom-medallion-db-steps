%sql
-- ============================================================
-- OPTION A - STEP 1 SETUP
-- Unity Catalog + Notebook friendly
-- Catalog: ecomsphere
--
-- What this script does:
-- 1) Creates schemas: meta, bronze, silver, gold
-- 2) Creates meta tables: cdc_watermark, job_audit
-- 3) Creates bronze CDC log tables for ALL source tables
-- 4) Initializes watermark rows for each table
-- ============================================================

USE CATALOG ecomsphere;

-- ------------------------------------------------------------
-- 1) Create Medallion schemas
-- ------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS meta;
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- ------------------------------------------------------------
-- 2) META TABLES
-- ------------------------------------------------------------

-- 2.1 Watermark table (query-based CDC control)
CREATE TABLE IF NOT EXISTS meta.cdc_watermark (
  source_schema STRING,         -- 'src_oltp_sim'
  source_table  STRING,         -- e.g. 'orders'
  watermark_col STRING,         -- 'updated_at' (we will use this)
  last_watermark_ts TIMESTAMP,  -- last extracted timestamp
  updated_at TIMESTAMP          -- audit
) USING DELTA;

-- 2.2 Job audit table (optional but very useful for teaching + ops)
CREATE TABLE IF NOT EXISTS meta.job_audit (
  cdc_run_id STRING,              -- airflow run_id or manual run label
  pipeline_step STRING,           -- e.g. 'extract_orders', 'merge_silver_orders'
  status STRING,                  -- 'STARTED','SUCCESS','FAILED'
  started_at TIMESTAMP,
  ended_at TIMESTAMP,
  records_read BIGINT,
  records_written BIGINT,
  message STRING
) USING DELTA;

-- ------------------------------------------------------------
-- 3) BRONZE CDC LOG TABLES (append-only)
-- Each CDC log table has:
--   - cdc_run_id
--   - cdc_extracted_at
--   - cdc_op  ('I','U','SD','HD')
-- plus original columns from source table
-- ------------------------------------------------------------

-- 3.1 brand
CREATE TABLE IF NOT EXISTS bronze.brand_cdc (
  cdc_run_id STRING,
  cdc_extracted_at TIMESTAMP,
  cdc_op STRING,
  brand_id BIGINT,
  brand_name STRING,
  description STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  is_deleted BOOLEAN
) USING DELTA;

-- 3.2 category
CREATE TABLE IF NOT EXISTS bronze.category_cdc (
  cdc_run_id STRING,
  cdc_extracted_at TIMESTAMP,
  cdc_op STRING,
  category_id BIGINT,
  category_name STRING,
  parent_category_id BIGINT,
  category_level INT,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  is_deleted BOOLEAN
) USING DELTA;

-- 3.3 warehouse
CREATE TABLE IF NOT EXISTS bronze.warehouse_cdc (
  cdc_run_id STRING,
  cdc_extracted_at TIMESTAMP,
  cdc_op STRING,
  warehouse_id BIGINT,
  warehouse_name STRING,
  city STRING,
  state STRING,
  country_code STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  is_deleted BOOLEAN
) USING DELTA;

-- 3.4 customer
CREATE TABLE IF NOT EXISTS bronze.customer_cdc (
  cdc_run_id STRING,
  cdc_extracted_at TIMESTAMP,
  cdc_op STRING,
  customer_id BIGINT,
  first_name STRING,
  last_name STRING,
  email STRING,
  phone_number STRING,
  status_code STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  is_deleted BOOLEAN
) USING DELTA;

-- 3.5 customer_address
CREATE TABLE IF NOT EXISTS bronze.customer_address_cdc (
  cdc_run_id STRING,
  cdc_extracted_at TIMESTAMP,
  cdc_op STRING,
  customer_address_id BIGINT,
  customer_id BIGINT,
  address_line1 STRING,
  address_line2 STRING,
  landmark STRING,
  city STRING,
  state STRING,
  postal_code STRING,
  country_code STRING,
  address_type_code STRING,
  is_default BOOLEAN,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  is_deleted BOOLEAN
) USING DELTA;

-- 3.6 product
CREATE TABLE IF NOT EXISTS bronze.product_cdc (
  cdc_run_id STRING,
  cdc_extracted_at TIMESTAMP,
  cdc_op STRING,
  product_id BIGINT,
  product_name STRING,
  brand_id BIGINT,
  default_category_id BIGINT,
  short_description STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  is_active BOOLEAN,
  is_deleted BOOLEAN
) USING DELTA;

-- 3.7 product_variant
CREATE TABLE IF NOT EXISTS bronze.product_variant_cdc (
  cdc_run_id STRING,
  cdc_extracted_at TIMESTAMP,
  cdc_op STRING,
  product_variant_id BIGINT,
  product_id BIGINT,
  sku STRING,
  variant_name STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  is_active BOOLEAN,
  is_deleted BOOLEAN
) USING DELTA;

-- 3.8 inventory_level
CREATE TABLE IF NOT EXISTS bronze.inventory_level_cdc (
  cdc_run_id STRING,
  cdc_extracted_at TIMESTAMP,
  cdc_op STRING,
  warehouse_id BIGINT,
  product_variant_id BIGINT,
  on_hand_qty INT,
  reserved_qty INT,
  last_updated_at TIMESTAMP,
  updated_at TIMESTAMP,
  is_deleted BOOLEAN
) USING DELTA;

-- 3.9 orders
CREATE TABLE IF NOT EXISTS bronze.orders_cdc (
  cdc_run_id STRING,
  cdc_extracted_at TIMESTAMP,
  cdc_op STRING,
  order_id BIGINT,
  customer_id BIGINT,
  order_date TIMESTAMP,
  order_status_code STRING,
  order_channel_code STRING,
  currency_code STRING,
  billing_address_id BIGINT,
  shipping_address_id BIGINT,
  total_item_amount DECIMAL(12,2),
  total_discount_amount DECIMAL(12,2),
  total_tax_amount DECIMAL(12,2),
  total_shipping_amount DECIMAL(12,2),
  grand_total_amount DECIMAL(12,2),
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  is_deleted BOOLEAN
) USING DELTA;

-- 3.10 order_item
CREATE TABLE IF NOT EXISTS bronze.order_item_cdc (
  cdc_run_id STRING,
  cdc_extracted_at TIMESTAMP,
  cdc_op STRING,
  order_item_id BIGINT,
  order_id BIGINT,
  product_variant_id BIGINT,
  quantity INT,
  base_unit_price DECIMAL(12,2),
  discount_amount DECIMAL(12,2),
  tax_amount DECIMAL(12,2),
  shipping_amount DECIMAL(12,2),
  line_total_amount DECIMAL(12,2),
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  is_deleted BOOLEAN
) USING DELTA;

-- 3.11 payment
CREATE TABLE IF NOT EXISTS bronze.payment_cdc (
  cdc_run_id STRING,
  cdc_extracted_at TIMESTAMP,
  cdc_op STRING,
  payment_id BIGINT,
  order_id BIGINT,
  payment_method_code STRING,
  payment_status_code STRING,
  amount DECIMAL(12,2),
  currency_code STRING,
  transaction_reference STRING,
  payment_date TIMESTAMP,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  is_deleted BOOLEAN
) USING DELTA;

-- 3.12 shipment
CREATE TABLE IF NOT EXISTS bronze.shipment_cdc (
  cdc_run_id STRING,
  cdc_extracted_at TIMESTAMP,
  cdc_op STRING,
  shipment_id BIGINT,
  order_id BIGINT,
  warehouse_id BIGINT,
  carrier_name STRING,
  shipment_status_code STRING,
  tracking_number STRING,
  shipped_date TIMESTAMP,
  delivered_date TIMESTAMP,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  is_deleted BOOLEAN
) USING DELTA;

-- ------------------------------------------------------------
-- 4) Initialize watermark rows (start from very old timestamp) (run only first time for setup)
-- ------------------------------------------------------------
INSERT OVERWRITE ecomsphere.meta.cdc_watermark
SELECT * FROM VALUES
  ('src_oltp_sim','brand','updated_at',TIMESTAMP('1900-01-01 00:00:00'), current_timestamp()),
  ('src_oltp_sim','category','updated_at',TIMESTAMP('1900-01-01 00:00:00'), current_timestamp()),
  ('src_oltp_sim','warehouse','updated_at',TIMESTAMP('1900-01-01 00:00:00'), current_timestamp()),
  ('src_oltp_sim','customer','updated_at',TIMESTAMP('1900-01-01 00:00:00'), current_timestamp()),
  ('src_oltp_sim','customer_address','updated_at',TIMESTAMP('1900-01-01 00:00:00'), current_timestamp()),
  ('src_oltp_sim','product','updated_at',TIMESTAMP('1900-01-01 00:00:00'), current_timestamp()),
  ('src_oltp_sim','product_variant','updated_at',TIMESTAMP('1900-01-01 00:00:00'), current_timestamp()),
  ('src_oltp_sim','inventory_level','updated_at',TIMESTAMP('1900-01-01 00:00:00'), current_timestamp()),
  ('src_oltp_sim','orders','updated_at',TIMESTAMP('1900-01-01 00:00:00'), current_timestamp()),
  ('src_oltp_sim','order_item','updated_at',TIMESTAMP('1900-01-01 00:00:00'), current_timestamp()),
  ('src_oltp_sim','payment','updated_at',TIMESTAMP('1900-01-01 00:00:00'), current_timestamp()),
  ('src_oltp_sim','shipment','updated_at',TIMESTAMP('1900-01-01 00:00:00'), current_timestamp());