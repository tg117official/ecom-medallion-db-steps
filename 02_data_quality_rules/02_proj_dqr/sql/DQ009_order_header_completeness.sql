-- ============================================================
-- DQ009 - Order header completeness
-- Completeness Template
-- ============================================================


CREATE OR REPLACE TEMP VIEW bronze_orders AS
SELECT * FROM VALUES
('O1001','C101',timestamp('2026-03-10 10:00:00'),'PLACED','APP','INR','A1','A2',100,10,5,20,115,timestamp('2026-03-10 10:01:00'),timestamp('2026-03-10 10:02:00'),false,timestamp('2026-03-10 10:03:00'),date('2026-03-10')),
('O1002',NULL,timestamp('2026-03-10 10:00:00'),'PLACED','APP','INR','A1','A2',100,10,5,20,115,timestamp('2026-03-10 10:01:00'),timestamp('2026-03-10 10:02:00'),false,timestamp('2026-03-10 10:03:00'),date('2026-03-10')),
(NULL,'C103',NULL,NULL,'APP','INR','A1','A2',100,10,5,20,115,timestamp('2026-03-10 10:01:00'),timestamp('2026-03-10 10:02:00'),false,timestamp('2026-03-10 10:03:00'),date('2026-03-10'))
AS t(order_id, customer_id, order_date, order_status_code, order_channel_code, currency_code, billing_address_id, shipping_address_id, total_item_amount, total_discount_amount, total_tax_amount, total_shipping_amount, grand_total_amount, created_at, updated_at, is_deleted, _ingest_ts, bronze_load_date);


CREATE OR REPLACE TEMP VIEW vw_bronze_orders_validated AS
SELECT
    *,
    CONCAT_WS(', '
        , CASE WHEN order_id IS NULL THEN 'order_id' END
        , CASE WHEN customer_id IS NULL THEN 'customer_id' END
        , CASE WHEN order_date IS NULL THEN 'order_date' END
        , CASE WHEN order_status_code IS NULL THEN 'order_status_code' END
    ) AS null_issue,
    CASE
        WHEN 1 = 0
 OR order_id IS NULL  OR customer_id IS NULL  OR order_date IS NULL  OR order_status_code IS NULL         THEN 1 ELSE 0
    END AS dq_null_violation_flag
FROM bronze_orders;

SELECT * FROM vw_bronze_orders_validated WHERE dq_null_violation_flag = 0;
SELECT * FROM vw_bronze_orders_validated WHERE dq_null_violation_flag = 1;

SELECT
    'DQ009_RUN_001' AS dq_run_id,
    'bronze_orders' AS table_name,
    'Order header completeness' AS rule_name,
    'Null / Completeness Checks' AS rule_category,
    COUNT(*) AS total_records_checked,
    SUM(CASE WHEN dq_null_violation_flag = 1 THEN 1 ELSE 0 END) AS failed_records_count,
    SUM(CASE WHEN dq_null_violation_flag = 0 THEN 1 ELSE 0 END) AS passed_records_count,
    ROUND(100.0 * SUM(CASE WHEN dq_null_violation_flag = 1 THEN 1 ELSE 0 END) / COUNT(*), 2) AS failure_percentage,
    CURRENT_TIMESTAMP() AS run_timestamp,
    'HIGH' AS severity,
    CASE WHEN SUM(CASE WHEN dq_null_violation_flag = 1 THEN 1 ELSE 0 END) > 0 THEN 'FAILED' ELSE 'PASSED' END AS status
FROM vw_bronze_orders_validated;

