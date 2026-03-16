-- ============================================================
-- DQ010 - Order item completeness
-- Completeness Template
-- ============================================================


CREATE OR REPLACE TEMP VIEW bronze_order_item AS
SELECT * FROM VALUES
('OI101','O1001','PV101',2,100,10,5,20,215,timestamp('2026-03-10 10:05:00'),timestamp('2026-03-10 10:06:00'),false,timestamp('2026-03-10 10:07:00'),date('2026-03-10')),
('OI102',NULL,'PV102',1,100,10,5,20,115,timestamp('2026-03-10 10:05:00'),timestamp('2026-03-10 10:06:00'),false,timestamp('2026-03-10 10:07:00'),date('2026-03-10')),
(NULL,'O1003',NULL,NULL,100,10,5,20,NULL,timestamp('2026-03-10 10:05:00'),timestamp('2026-03-10 10:06:00'),false,timestamp('2026-03-10 10:07:00'),date('2026-03-10'))
AS t(order_item_id, order_id, product_variant_id, quantity, base_unit_price, discount_amount, tax_amount, shipping_amount, line_total_amount, created_at, updated_at, is_deleted, _ingest_ts, bronze_load_date);


CREATE OR REPLACE TEMP VIEW vw_bronze_order_item_validated AS
SELECT
    *,
    CONCAT_WS(', '
        , CASE WHEN order_item_id IS NULL THEN 'order_item_id' END
        , CASE WHEN order_id IS NULL THEN 'order_id' END
        , CASE WHEN product_variant_id IS NULL THEN 'product_variant_id' END
        , CASE WHEN quantity IS NULL THEN 'quantity' END
        , CASE WHEN line_total_amount IS NULL THEN 'line_total_amount' END
    ) AS null_issue,
    CASE
        WHEN 1 = 0
 OR order_item_id IS NULL  OR order_id IS NULL  OR product_variant_id IS NULL  OR quantity IS NULL  OR line_total_amount IS NULL         THEN 1 ELSE 0
    END AS dq_null_violation_flag
FROM bronze_order_item;

SELECT * FROM vw_bronze_order_item_validated WHERE dq_null_violation_flag = 0;
SELECT * FROM vw_bronze_order_item_validated WHERE dq_null_violation_flag = 1;

SELECT
    'DQ010_RUN_001' AS dq_run_id,
    'bronze_order_item' AS table_name,
    'Order item completeness' AS rule_name,
    'Null / Completeness Checks' AS rule_category,
    COUNT(*) AS total_records_checked,
    SUM(CASE WHEN dq_null_violation_flag = 1 THEN 1 ELSE 0 END) AS failed_records_count,
    SUM(CASE WHEN dq_null_violation_flag = 0 THEN 1 ELSE 0 END) AS passed_records_count,
    ROUND(100.0 * SUM(CASE WHEN dq_null_violation_flag = 1 THEN 1 ELSE 0 END) / COUNT(*), 2) AS failure_percentage,
    CURRENT_TIMESTAMP() AS run_timestamp,
    'HIGH' AS severity,
    CASE WHEN SUM(CASE WHEN dq_null_violation_flag = 1 THEN 1 ELSE 0 END) > 0 THEN 'FAILED' ELSE 'PASSED' END AS status
FROM vw_bronze_order_item_validated;

