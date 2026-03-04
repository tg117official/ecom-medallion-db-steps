%sql
-- =====================================================================
-- OPTION A (Query-based CDC) - SINGLE TABLE DEMO
-- Table: src_oltp_sim.orders  -->  bronze.orders_cdc
--
-- What students will learn (step-by-step):
-- 1) Initial load (first CDC run) from source to bronze CDC log
-- 2) Insert new rows in source, then CDC run detects + ingests them
-- 3) Update existing rows in source, then CDC run detects + ingests them
-- 4) Soft delete rows in source (is_deleted=true), then CDC run ingests them
--
-- Notes:
-- - Source is Delta table, so we use updated_at watermark.
-- - Bronze CDC log is append-only with cdc_run_id, cdc_extracted_at, cdc_op.
-- - For query-based CDC:
--     * Any row with updated_at > watermark is considered a "change event".
--     * We classify cdc_op as:
--         - 'SD' if is_deleted = true
--         - otherwise 'I/U' (query-based cannot truly differentiate insert vs update
--           without extra logic; we keep it as 'IU' for simplicity in teaching).
-- - Later, in Silver, we will MERGE using latest updated_at.
-- =====================================================================

USE CATALOG ecomsphere;
USE SCHEMA src_oltp_sim;

-- ---------------------------------------------------------------------
-- 0) Ensure the watermark row exists for 'orders'
--    (If you already ran STEP-1 setup, this will just keep it consistent)
-- ---------------------------------------------------------------------
MERGE INTO ecomsphere.meta.cdc_watermark t
USING (
  SELECT * FROM VALUES
    ('src_oltp_sim','orders','updated_at', TIMESTAMP('1900-01-01 00:00:00'), current_timestamp())
) s (source_schema, source_table, watermark_col, last_watermark_ts, updated_at)
ON t.source_schema = s.source_schema AND t.source_table = s.source_table
WHEN MATCHED THEN UPDATE SET
  t.watermark_col = s.watermark_col,
  t.updated_at = current_timestamp()
WHEN NOT MATCHED THEN INSERT *;

-- ---------------------------------------------------------------------
-- Helper: create a view that exposes the current watermark for orders
-- (Students can SELECT from this view to see watermark moving)
-- ---------------------------------------------------------------------
CREATE OR REPLACE TEMP VIEW vw_orders_watermark AS
SELECT last_watermark_ts
FROM ecomsphere.meta.cdc_watermark
WHERE source_schema = 'src_oltp_sim' AND source_table = 'orders';

select * from vw_orders_watermark ;

-- =====================================================================
-- 1) CDC RUN #1 : INITIAL LOAD
-- =====================================================================

-- ---------------------------------------------------------------------
-- 1.1 Define a run id for easy tracking in bronze
--     (In Airflow you’ll pass run_id; here we hardcode a label)
-- ---------------------------------------------------------------------
CREATE OR REPLACE TEMP VIEW vw_cdc_run_01 AS
SELECT 'RUN_01_INITIAL_LOAD' AS cdc_run_id;

select * from vw_cdc_run_01 ;
-- ---------------------------------------------------------------------
-- 1.2 Ingest changes since watermark into bronze.orders_cdc
--     Because watermark starts at 1900-01-01, this will load ALL rows.
-- ---------------------------------------------------------------------
INSERT INTO ecomsphere.bronze.orders_cdc
SELECT
  (SELECT cdc_run_id FROM vw_cdc_run_01)     AS cdc_run_id,
  current_timestamp()                       AS cdc_extracted_at,
  CASE WHEN o.is_deleted = true THEN 'SD' ELSE 'IU' END AS cdc_op,
  o.order_id,
  o.customer_id,
  o.order_date,
  o.order_status_code,
  o.order_channel_code,
  o.currency_code,
  o.billing_address_id,
  o.shipping_address_id,
  o.total_item_amount,
  o.total_discount_amount,
  o.total_tax_amount,
  o.total_shipping_amount,
  o.grand_total_amount,
  o.created_at,
  o.updated_at,
  o.is_deleted
FROM src_oltp_sim.orders o
WHERE o.updated_at > (SELECT last_watermark_ts FROM vw_orders_watermark);

select * from ecomsphere.bronze.orders_cdc ;

-- ---------------------------------------------------------------------
-- 1.3 Update watermark to the max(updated_at) we just ingested
--     This makes the next CDC run incremental.
-- ---------------------------------------------------------------------
UPDATE ecomsphere.meta.cdc_watermark
SET last_watermark_ts = (
      SELECT COALESCE(MAX(updated_at), (SELECT last_watermark_ts FROM vw_orders_watermark))
      FROM ecomsphere.bronze.orders_cdc
      WHERE cdc_run_id = (SELECT cdc_run_id FROM vw_cdc_run_01)
    ),
    updated_at = current_timestamp()
WHERE source_schema='src_oltp_sim' AND source_table='orders';

select * from ecomsphere.meta.cdc_watermark ;

-- ---------------------------------------------------------------------
-- 1.4 Quick check: show what came into bronze in run #1
-- ---------------------------------------------------------------------
SELECT cdc_run_id, cdc_op, order_id, order_status_code, updated_at, is_deleted
FROM ecomsphere.bronze.orders_cdc
WHERE cdc_run_id = (SELECT cdc_run_id FROM vw_cdc_run_01)
ORDER BY updated_at, order_id;

-- =====================================================================
-- 2) SIMULATE SOURCE CHANGES: INSERT NEW ORDERS
-- =====================================================================

-- ---------------------------------------------------------------------
-- 2.1 Insert 2 new orders into source (new order_ids: 9011, 9012)
--     Ensure referenced keys exist (customer_id and address ids from seed).
-- ---------------------------------------------------------------------
INSERT INTO src_oltp_sim.orders VALUES
  (9011, 1000, current_timestamp(), 'PENDING', 'WEB', 'INR', 5000, 5000,
   999.00,  0.00, 0.00, 50.00, 1049.00, current_timestamp(), current_timestamp(), false),

  (9012, 1001, current_timestamp(), 'PENDING', 'MOBILE', 'INR', 5001, 5001,
   3499.00, 200.00, 0.00, 50.00, 3349.00, current_timestamp(), current_timestamp(), false);

-- =====================================================================
-- 3) CDC RUN #2 : CAPTURE INSERTS
-- =====================================================================

CREATE OR REPLACE TEMP VIEW vw_cdc_run_02 AS
SELECT 'RUN_02_AFTER_INSERTS' AS cdc_run_id;

INSERT INTO ecomsphere.bronze.orders_cdc
SELECT
  (SELECT cdc_run_id FROM vw_cdc_run_02)     AS cdc_run_id,
  current_timestamp()                       AS cdc_extracted_at,
  CASE WHEN o.is_deleted = true THEN 'SD' ELSE 'IU' END AS cdc_op,
  o.order_id,
  o.customer_id,
  o.order_date,
  o.order_status_code,
  o.order_channel_code,
  o.currency_code,
  o.billing_address_id,
  o.shipping_address_id,
  o.total_item_amount,
  o.total_discount_amount,
  o.total_tax_amount,
  o.total_shipping_amount,
  o.grand_total_amount,
  o.created_at,
  o.updated_at,
  o.is_deleted
FROM src_oltp_sim.orders o
WHERE o.updated_at > (SELECT last_watermark_ts FROM vw_orders_watermark);

select * from ecomsphere.bronze.orders_cdc ;

UPDATE ecomsphere.meta.cdc_watermark
SET last_watermark_ts = (
      SELECT COALESCE(MAX(updated_at), (SELECT last_watermark_ts FROM vw_orders_watermark))
      FROM ecomsphere.bronze.orders_cdc
      WHERE cdc_run_id = (SELECT cdc_run_id FROM vw_cdc_run_02)
    ),
    updated_at = current_timestamp()
WHERE source_schema='src_oltp_sim' AND source_table='orders';

SELECT cdc_run_id, cdc_op, order_id, order_status_code, updated_at, is_deleted
FROM ecomsphere.bronze.orders_cdc
WHERE cdc_run_id = (SELECT cdc_run_id FROM vw_cdc_run_02)
ORDER BY updated_at, order_id;



-- =====================================================================
-- 4) SIMULATE SOURCE CHANGES: UPDATE EXISTING ORDERS
-- =====================================================================

-- ---------------------------------------------------------------------
-- 4.1 Update status on existing orders (update updates updated_at)
-- ---------------------------------------------------------------------
UPDATE src_oltp_sim.orders
SET order_status_code = 'PAID',
    updated_at = current_timestamp()
WHERE order_id IN (9000, 9011);

-- =====================================================================
-- 5) CDC RUN #3 : CAPTURE UPDATES
-- =====================================================================

CREATE OR REPLACE TEMP VIEW vw_cdc_run_03 AS
SELECT 'RUN_03_AFTER_UPDATES' AS cdc_run_id;

INSERT INTO ecomsphere.bronze.orders_cdc
SELECT
  (SELECT cdc_run_id FROM vw_cdc_run_03)     AS cdc_run_id,
  current_timestamp()                       AS cdc_extracted_at,
  CASE WHEN o.is_deleted = true THEN 'SD' ELSE 'IU' END AS cdc_op,
  o.order_id,
  o.customer_id,
  o.order_date,
  o.order_status_code,
  o.order_channel_code,
  o.currency_code,
  o.billing_address_id,
  o.shipping_address_id,
  o.total_item_amount,
  o.total_discount_amount,
  o.total_tax_amount,
  o.total_shipping_amount,
  o.grand_total_amount,
  o.created_at,
  o.updated_at,
  o.is_deleted
FROM src_oltp_sim.orders o
WHERE o.updated_at > (SELECT last_watermark_ts FROM vw_orders_watermark);

UPDATE ecomsphere.meta.cdc_watermark
SET last_watermark_ts = (
      SELECT COALESCE(MAX(updated_at), (SELECT last_watermark_ts FROM vw_orders_watermark))
      FROM ecomsphere.bronze.orders_cdc
      WHERE cdc_run_id = (SELECT cdc_run_id FROM vw_cdc_run_03)
    ),
    updated_at = current_timestamp()
WHERE source_schema='src_oltp_sim' AND source_table='orders';

SELECT cdc_run_id, cdc_op, order_id, order_status_code, updated_at, is_deleted
FROM ecomsphere.bronze.orders_cdc
WHERE cdc_run_id = (SELECT cdc_run_id FROM vw_cdc_run_03)
ORDER BY updated_at, order_id;

-- =====================================================================
-- 6) SIMULATE SOURCE CHANGES: SOFT DELETE
-- =====================================================================

-- ---------------------------------------------------------------------
-- 6.1 Soft delete an order (set is_deleted = true + touch updated_at)
-- ---------------------------------------------------------------------
UPDATE src_oltp_sim.orders
SET is_deleted = true,
    updated_at = current_timestamp()
WHERE order_id = 9012;

-- =====================================================================
-- 7) CDC RUN #4 : CAPTURE SOFT DELETES
-- =====================================================================

CREATE OR REPLACE TEMP VIEW vw_cdc_run_04 AS
SELECT 'RUN_04_AFTER_SOFT_DELETE' AS cdc_run_id;

INSERT INTO ecomsphere.bronze.orders_cdc
SELECT
  (SELECT cdc_run_id FROM vw_cdc_run_04)     AS cdc_run_id,
  current_timestamp()                       AS cdc_extracted_at,
  CASE WHEN o.is_deleted = true THEN 'SD' ELSE 'IU' END AS cdc_op,
  o.order_id,
  o.customer_id,
  o.order_date,
  o.order_status_code,
  o.order_channel_code,
  o.currency_code,
  o.billing_address_id,
  o.shipping_address_id,
  o.total_item_amount,
  o.total_discount_amount,
  o.total_tax_amount,
  o.total_shipping_amount,
  o.grand_total_amount,
  o.created_at,
  o.updated_at,
  o.is_deleted
FROM src_oltp_sim.orders o
WHERE o.updated_at > (SELECT last_watermark_ts FROM vw_orders_watermark);

UPDATE ecomsphere.meta.cdc_watermark
SET last_watermark_ts = (
      SELECT COALESCE(MAX(updated_at), (SELECT last_watermark_ts FROM vw_orders_watermark))
      FROM ecomsphere.bronze.orders_cdc
      WHERE cdc_run_id = (SELECT cdc_run_id FROM vw_cdc_run_04)
    ),
    updated_at = current_timestamp()
WHERE source_schema='src_oltp_sim' AND source_table='orders';

SELECT cdc_run_id, cdc_op, order_id, order_status_code, updated_at, is_deleted
FROM ecomsphere.bronze.orders_cdc
WHERE cdc_run_id = (SELECT cdc_run_id FROM vw_cdc_run_04)
ORDER BY updated_at, order_id;

-- =====================================================================
-- 8) FINAL VIEW: Show ALL CDC events for orders so far
-- =====================================================================
SELECT cdc_run_id, cdc_extracted_at, cdc_op, order_id, order_status_code, updated_at, is_deleted
FROM ecomsphere.bronze.orders_cdc
ORDER BY cdc_extracted_at, order_id;