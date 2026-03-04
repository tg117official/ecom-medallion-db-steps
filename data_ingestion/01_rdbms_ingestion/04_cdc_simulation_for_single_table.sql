%sql
USE CATALOG ecomsphere;
USE SCHEMA src_oltp_sim;

-- ============================================================
-- CHANGE SIMULATION FOR 3 CDC JOB RUNS (ORDERS)
-- Run these blocks one-by-one:
--
-- Block A (RUN_01 changes) -> then run your CDC notebook
-- Block B (RUN_02 changes) -> then run your CDC notebook
-- Block C (RUN_03 changes) -> then run your CDC notebook
--
-- Notes:
-- - We use order_ids 91001..91006 to avoid clashing with your seed (9000..)
-- - Updates touch updated_at so they are detected by query-based CDC
-- - Soft delete uses is_deleted=true and updated_at touch
-- ============================================================


-- ============================================================
-- BLOCK A: Changes for CDC RUN #1 (INSERT delta)
-- ============================================================

-- A1) Insert 2 NEW orders (delta = inserts)
INSERT INTO ecomsphere.src_oltp_sim.orders VALUES
  (91001, 1000, current_timestamp(), 'PENDING', 'WEB',    'INR', 5000, 5000,
   1999.00, 0.00, 0.00, 50.00, 2049.00, current_timestamp(), current_timestamp(), false),

  (91002, 1001, current_timestamp(), 'PENDING', 'MOBILE', 'INR', 5001, 5001,
   3499.00, 200.00, 0.00, 50.00, 3349.00, current_timestamp(), current_timestamp(), false);

-- A2) Quick check (optional)
SELECT order_id, order_status_code, updated_at, is_deleted
FROM ecomsphere.src_oltp_sim.orders
WHERE order_id IN (91001, 91002)
ORDER BY order_id;


-- ============================================================
-- BLOCK B: Changes for CDC RUN #2 (UPDATE delta)
-- ============================================================

-- B1) Update existing orders (delta = updates)
-- (we update the rows inserted in Block A)
UPDATE ecomsphere.src_oltp_sim.orders
SET order_status_code = 'PAID',
    total_discount_amount = total_discount_amount + 50.00,
    grand_total_amount = grand_total_amount - 50.00,
    updated_at = current_timestamp()
WHERE order_id IN (91001, 91002);

-- B2) Also update 1 old existing order (choose one that exists in your seed, e.g. 9000)
-- If 9000 doesn't exist in your environment, change it to any existing order_id.
UPDATE ecomsphere.src_oltp_sim.orders
SET order_status_code = 'PAID',
    updated_at = current_timestamp()
WHERE order_id = 9000;

-- B3) Quick check (optional)
SELECT order_id, order_status_code, grand_total_amount, updated_at, is_deleted
FROM ecomsphere.src_oltp_sim.orders
WHERE order_id IN (91001, 91002, 9000)
ORDER BY order_id;


-- ============================================================
-- BLOCK C: Changes for CDC RUN #3 (SOFT DELETE delta)
-- ============================================================

-- C1) Insert 1 more new order (optional extra delta in this run)
INSERT INTO ecomsphere.src_oltp_sim.orders VALUES
  (91003, 1003, current_timestamp(), 'PENDING', 'WEB', 'INR', 5003, 5000,
   999.00, 0.00, 0.00, 50.00, 1049.00, current_timestamp(), current_timestamp(), false);

-- C2) Soft delete one of the earlier inserted orders
UPDATE ecomsphere.src_oltp_sim.orders
SET is_deleted = true,
    updated_at = current_timestamp()
WHERE order_id = 91002;

-- C3) Soft delete one older order as well (optional)
-- If 9001 doesn't exist, change it to any existing order_id.
UPDATE ecomsphere.src_oltp_sim.orders
SET is_deleted = true,
    updated_at = current_timestamp()
WHERE order_id = 9001;

-- C4) Quick check (optional)
SELECT order_id, order_status_code, updated_at, is_deleted
FROM ecomsphere.src_oltp_sim.orders
WHERE order_id IN (91002, 91003, 9001)
ORDER BY order_id;