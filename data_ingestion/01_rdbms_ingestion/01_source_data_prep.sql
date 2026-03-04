%sql
-- ============================================================
-- OLTP SOURCE SIMULATION (DELTA) FOR MEDALLION DEMO
-- ONLY: CREATE TABLES + INSERT SEED DATA
-- Unity Catalog + Notebook safe (copy-paste runnable)
-- ============================================================

-- ------------------------------------------------------------
-- 0) Use catalog + create schema
-- ------------------------------------------------------------
USE CATALOG ecomsphere;
CREATE SCHEMA IF NOT EXISTS src_oltp_sim;
USE SCHEMA src_oltp_sim;

-- ------------------------------------------------------------
-- 1) Reference / Master tables (parents)
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS src_oltp_sim.brand (
  brand_id BIGINT,
  brand_name STRING,
  description STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  is_deleted BOOLEAN
) USING DELTA;

CREATE TABLE IF NOT EXISTS src_oltp_sim.category (
  category_id BIGINT,
  category_name STRING,
  parent_category_id BIGINT,
  category_level INT,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  is_deleted BOOLEAN
) USING DELTA;

CREATE TABLE IF NOT EXISTS src_oltp_sim.warehouse (
  warehouse_id BIGINT,
  warehouse_name STRING,
  city STRING,
  state STRING,
  country_code STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  is_deleted BOOLEAN
) USING DELTA;

-- ------------------------------------------------------------
-- 2) Customer tables
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS src_oltp_sim.customer (
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

CREATE TABLE IF NOT EXISTS src_oltp_sim.customer_address (
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

-- ------------------------------------------------------------
-- 3) Product tables
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS src_oltp_sim.product (
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

CREATE TABLE IF NOT EXISTS src_oltp_sim.product_variant (
  product_variant_id BIGINT,
  product_id BIGINT,
  sku STRING,
  variant_name STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  is_active BOOLEAN,
  is_deleted BOOLEAN
) USING DELTA;

-- ------------------------------------------------------------
-- 4) Inventory
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS src_oltp_sim.inventory_level (
  warehouse_id BIGINT,
  product_variant_id BIGINT,
  on_hand_qty INT,
  reserved_qty INT,
  last_updated_at TIMESTAMP,
  updated_at TIMESTAMP,
  is_deleted BOOLEAN
) USING DELTA;

-- ------------------------------------------------------------
-- 5) Transaction tables
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS src_oltp_sim.orders (
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

CREATE TABLE IF NOT EXISTS src_oltp_sim.order_item (
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

CREATE TABLE IF NOT EXISTS src_oltp_sim.payment (
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

CREATE TABLE IF NOT EXISTS src_oltp_sim.shipment (
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

-- ============================================================
-- SEED DATA LOAD (Notebook-safe)
-- IMPORTANT: We use INSERT OVERWRITE for master tables to make
-- reruns deterministic without MERGE syntax issues.
-- ============================================================

-- ------------------------------------------------------------
-- A) Master data (rerunnable)
-- ------------------------------------------------------------

INSERT OVERWRITE src_oltp_sim.brand
SELECT * FROM VALUES
  (1, 'Acme',    'General consumer brand', current_timestamp(), current_timestamp(), false),
  (2, 'ZenTech', 'Electronics brand',      current_timestamp(), current_timestamp(), false),
  (3, 'HomePro', 'Home and kitchen brand', current_timestamp(), current_timestamp(), false);

INSERT OVERWRITE src_oltp_sim.category
SELECT * FROM VALUES
  (10, 'Electronics', CAST(NULL AS BIGINT), 1, current_timestamp(), current_timestamp(), false),
  (11, 'Mobiles',     10,                   2, current_timestamp(), current_timestamp(), false),
  (12, 'Accessories', 10,                   2, current_timestamp(), current_timestamp(), false),
  (20, 'Home',        CAST(NULL AS BIGINT), 1, current_timestamp(), current_timestamp(), false),
  (21, 'Kitchen',     20,                   2, current_timestamp(), current_timestamp(), false);

INSERT OVERWRITE src_oltp_sim.warehouse
SELECT * FROM VALUES
  (100, 'WH_Pune_01',   'Pune',   'MH', 'IND', current_timestamp(), current_timestamp(), false),
  (101, 'WH_Mumbai_01', 'Mumbai', 'MH', 'IND', current_timestamp(), current_timestamp(), false),
  (102, 'WH_Delhi_01',  'Delhi',  'DL', 'IND', current_timestamp(), current_timestamp(), false);

INSERT OVERWRITE src_oltp_sim.customer
SELECT * FROM VALUES
  (1000, 'Amit', 'Sharma', 'amit@example.com', '+91-9000000001', 'ACTIVE', current_timestamp(), current_timestamp(), false),
  (1001, 'Neha', 'Patil',  'neha@example.com', '+91-9000000002', 'ACTIVE', current_timestamp(), current_timestamp(), false),
  (1002, 'Ravi', 'Kumar',  'ravi@example.com', '+91-9000000003', 'ACTIVE', current_timestamp(), current_timestamp(), false),
  (1003, 'Sara', 'Iyer',   'sara@example.com', '+91-9000000004', 'ACTIVE', current_timestamp(), current_timestamp(), false);

INSERT OVERWRITE src_oltp_sim.customer_address
SELECT * FROM VALUES
  (5000, 1000, 'MG Road',       'Near Metro',   'Camp',     'Pune',   'MH', '411001', 'IND', 'HOME',   true,  current_timestamp(), current_timestamp(), false),
  (5001, 1001, 'Link Road',     'Andheri West', 'Opp Mall', 'Mumbai', 'MH', '400053', 'IND', 'HOME',   true,  current_timestamp(), current_timestamp(), false),
  (5002, 1002, 'Civil Lines',   CAST(NULL AS STRING), CAST(NULL AS STRING), 'Nagpur', 'MH', '440001', 'IND', 'HOME',   true,  current_timestamp(), current_timestamp(), false),
  (5003, 1003, 'Baner High St', 'Near IT Park', CAST(NULL AS STRING),       'Pune',   'MH', '411045', 'IND', 'OFFICE', true,  current_timestamp(), current_timestamp(), false);

INSERT OVERWRITE src_oltp_sim.product
SELECT * FROM VALUES
  (2000, 'Smartphone X',     2, 11, 'Mid-range smartphone', current_timestamp(), current_timestamp(), true,  false),
  (2001, 'Phone Case',       1, 12, 'Protective case',      current_timestamp(), current_timestamp(), true,  false),
  (2002, 'Wireless Earbuds', 2, 12, 'Bluetooth earbuds',    current_timestamp(), current_timestamp(), true,  false),
  (2003, 'Mixer Grinder',    3, 21, 'Kitchen appliance',    current_timestamp(), current_timestamp(), true,  false);

INSERT OVERWRITE src_oltp_sim.product_variant
SELECT * FROM VALUES
  (3000, 2000, 'SKU-SMX-BLK-128', 'Black / 128GB', current_timestamp(), current_timestamp(), true, false),
  (3001, 2000, 'SKU-SMX-BLU-256', 'Blue / 256GB',  current_timestamp(), current_timestamp(), true, false),
  (3002, 2001, 'SKU-CASE-RED',    'Red Case',      current_timestamp(), current_timestamp(), true, false),
  (3003, 2002, 'SKU-EARBUD-WHT',  'White Earbuds', current_timestamp(), current_timestamp(), true, false),
  (3004, 2003, 'SKU-MIX-750W',    '750W Mixer',    current_timestamp(), current_timestamp(), true, false);

INSERT OVERWRITE src_oltp_sim.inventory_level
SELECT * FROM VALUES
  (100, 3000,  50,  5, current_timestamp(), current_timestamp(), false),
  (100, 3001,  20,  2, current_timestamp(), current_timestamp(), false),
  (101, 3002, 120, 10, current_timestamp(), current_timestamp(), false),
  (101, 3003,  70,  4, current_timestamp(), current_timestamp(), false),
  (102, 3004,  35,  1, current_timestamp(), current_timestamp(), false);

-- ------------------------------------------------------------
-- B) Transaction data (DELETE + INSERT = deterministic reruns)
-- ------------------------------------------------------------

DELETE FROM src_oltp_sim.shipment   WHERE order_id IN (9000, 9001, 9002);
DELETE FROM src_oltp_sim.payment    WHERE order_id IN (9000, 9001, 9002);
DELETE FROM src_oltp_sim.order_item WHERE order_id IN (9000, 9001, 9002);
DELETE FROM src_oltp_sim.orders     WHERE order_id IN (9000, 9001, 9002);

INSERT INTO src_oltp_sim.orders VALUES
  (9000, 1000, current_timestamp(), 'PENDING', 'WEB',    'INR', 5000, 5000,
   60998.00, 1000.00, 0.00, 100.00, 60098.00, current_timestamp(), current_timestamp(), false),
  (9001, 1001, current_timestamp(), 'PENDING', 'MOBILE', 'INR', 5001, 5001,
   1998.00,  0.00,    0.00,  50.00,  2048.00, current_timestamp(), current_timestamp(), false),
  (9002, 1003, current_timestamp(), 'PENDING', 'WEB',    'INR', 5003, 5000,
   3499.00,  200.00,  0.00,  50.00,  3349.00, current_timestamp(), current_timestamp(), false);

INSERT INTO src_oltp_sim.order_item VALUES
  (91000, 9000, 3000, 1, 59999.00, 1000.00, 0.00, 100.00, 59099.00, current_timestamp(), current_timestamp(), false),
  (91001, 9000, 3002, 1,   999.00,    0.00, 0.00,   0.00,   999.00, current_timestamp(), current_timestamp(), false),
  (91002, 9001, 3002, 2,   999.00,    0.00, 0.00,  50.00,  2048.00, current_timestamp(), current_timestamp(), false),
  (91003, 9002, 3003, 1,  3499.00,  200.00, 0.00,  50.00,  3349.00, current_timestamp(), current_timestamp(), false);

INSERT INTO src_oltp_sim.payment VALUES
  (92000, 9000, 'UPI',  'PENDING', 60098.00, 'INR', NULL, current_timestamp(), current_timestamp(), current_timestamp(), false),
  (92001, 9001, 'CARD', 'PENDING',  2048.00, 'INR', NULL, current_timestamp(), current_timestamp(), current_timestamp(), false),
  (92002, 9002, 'UPI',  'PENDING',  3349.00, 'INR', NULL, current_timestamp(), current_timestamp(), current_timestamp(), false);

INSERT INTO src_oltp_sim.shipment VALUES
  (93000, 9000, 100, 'BlueDart',    'CREATED', 'TRK-9000', NULL, NULL, current_timestamp(), current_timestamp(), false),
  (93001, 9001, 101, 'Delhivery',   'CREATED', 'TRK-9001', NULL, NULL, current_timestamp(), current_timestamp(), false),
  (93002, 9002, 100, 'EcomExpress', 'CREATED', 'TRK-9002', NULL, NULL, current_timestamp(), current_timestamp(), false);

-- ============================================================
-- QUICK DATA CHECKS (LIMITED ROWS PER TABLE)
-- Unity Catalog + Notebook safe
-- ============================================================

USE CATALOG ecomsphere;
USE SCHEMA src_oltp_sim;

SELECT * FROM src_oltp_sim.brand           LIMIT 20;
SELECT * FROM src_oltp_sim.category        LIMIT 20;
SELECT * FROM src_oltp_sim.warehouse       LIMIT 20;

SELECT * FROM src_oltp_sim.customer        LIMIT 20;
SELECT * FROM src_oltp_sim.customer_address LIMIT 20;

SELECT * FROM src_oltp_sim.product         LIMIT 20;
SELECT * FROM src_oltp_sim.product_variant LIMIT 20;

SELECT * FROM src_oltp_sim.inventory_level LIMIT 20;

SELECT * FROM src_oltp_sim.orders          LIMIT 20;
SELECT * FROM src_oltp_sim.order_item      LIMIT 20;

SELECT * FROM src_oltp_sim.payment         LIMIT 20;
SELECT * FROM src_oltp_sim.shipment        LIMIT 20;
-- ============================================================
-- END
-- ============================================================