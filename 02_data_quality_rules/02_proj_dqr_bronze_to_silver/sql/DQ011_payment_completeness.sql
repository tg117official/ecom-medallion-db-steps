-- ============================================================
-- DQ011 - Payment completeness
-- Completeness Template
-- ============================================================


CREATE OR REPLACE TEMP VIEW bronze_payment AS
SELECT * FROM VALUES
('PAY101','O1001','UPI','SUCCESS',115,'INR','TXN1',timestamp('2026-03-10 10:10:00'),timestamp('2026-03-10 10:11:00'),timestamp('2026-03-10 10:12:00'),false,timestamp('2026-03-10 10:13:00'),date('2026-03-10')),
('PAY102',NULL,'CARD',NULL,200,'INR','TXN2',timestamp('2026-03-10 10:10:00'),timestamp('2026-03-10 10:11:00'),timestamp('2026-03-10 10:12:00'),false,timestamp('2026-03-10 10:13:00'),date('2026-03-10')),
(NULL,'O1003',NULL,NULL,NULL,'INR','TXN3',NULL,timestamp('2026-03-10 10:11:00'),timestamp('2026-03-10 10:12:00'),false,timestamp('2026-03-10 10:13:00'),date('2026-03-10'))
AS t(payment_id, order_id, payment_method_code, payment_status_code, amount, currency_code, transaction_reference, payment_date, created_at, updated_at, is_deleted, _ingest_ts, bronze_load_date);


CREATE OR REPLACE TEMP VIEW vw_bronze_payment_validated AS
SELECT
    *,
    CONCAT_WS(', '
        , CASE WHEN payment_id IS NULL THEN 'payment_id' END
        , CASE WHEN order_id IS NULL THEN 'order_id' END
        , CASE WHEN payment_method_code IS NULL THEN 'payment_method_code' END
        , CASE WHEN amount IS NULL THEN 'amount' END
        , CASE WHEN payment_status_code IS NULL THEN 'payment_status_code' END
    ) AS null_issue,
    CASE
        WHEN 1 = 0
 OR payment_id IS NULL  OR order_id IS NULL  OR payment_method_code IS NULL  OR amount IS NULL  OR payment_status_code IS NULL         THEN 1 ELSE 0
    END AS dq_null_violation_flag
FROM bronze_payment;

SELECT * FROM vw_bronze_payment_validated WHERE dq_null_violation_flag = 0;
SELECT * FROM vw_bronze_payment_validated WHERE dq_null_violation_flag = 1;

SELECT
    'DQ011_RUN_001' AS dq_run_id,
    'bronze_payment' AS table_name,
    'Payment completeness' AS rule_name,
    'Null / Completeness Checks' AS rule_category,
    COUNT(*) AS total_records_checked,
    SUM(CASE WHEN dq_null_violation_flag = 1 THEN 1 ELSE 0 END) AS failed_records_count,
    SUM(CASE WHEN dq_null_violation_flag = 0 THEN 1 ELSE 0 END) AS passed_records_count,
    ROUND(100.0 * SUM(CASE WHEN dq_null_violation_flag = 1 THEN 1 ELSE 0 END) / COUNT(*), 2) AS failure_percentage,
    CURRENT_TIMESTAMP() AS run_timestamp,
    'HIGH' AS severity,
    CASE WHEN SUM(CASE WHEN dq_null_violation_flag = 1 THEN 1 ELSE 0 END) > 0 THEN 'FAILED' ELSE 'PASSED' END AS status
FROM vw_bronze_payment_validated;

