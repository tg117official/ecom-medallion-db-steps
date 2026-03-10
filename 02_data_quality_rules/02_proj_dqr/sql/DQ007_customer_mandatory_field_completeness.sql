-- ============================================================
-- DQ007 - Customer mandatory field completeness
-- Completeness Template
-- ============================================================


CREATE OR REPLACE TEMP VIEW bronze_customer AS
SELECT * FROM VALUES
('C101','Amit','Patil','amit@gmail.com','9876543210','ACTIVE',timestamp('2026-03-10 09:00:00'),timestamp('2026-03-10 09:05:00'),false,timestamp('2026-03-10 09:10:00'),date('2026-03-10')),
('C102',NULL,'Shah','  ',NULL,'ACTIVE',timestamp('2026-03-10 09:00:00'),timestamp('2026-03-10 09:05:00'),false,timestamp('2026-03-10 09:10:00'),date('2026-03-10')),
(NULL,'Ravi','Kumar','ravi@gmail.com','9999999999',NULL,timestamp('2026-03-10 09:00:00'),timestamp('2026-03-10 09:05:00'),false,timestamp('2026-03-10 09:10:00'),date('2026-03-10'))
AS t(customer_id, first_name, last_name, email, phone_number, status_code, created_at, updated_at, is_deleted, _ingest_ts, bronze_load_date);


CREATE OR REPLACE TEMP VIEW vw_bronze_customer_validated AS
SELECT
    *,
    CONCAT_WS(', '
        , CASE WHEN customer_id IS NULL THEN 'customer_id' END
        , CASE WHEN first_name IS NULL THEN 'first_name' END
        , CASE WHEN email IS NULL THEN 'email' END
        , CASE WHEN status_code IS NULL THEN 'status_code' END
    ) AS null_issue,
    CASE
        WHEN 1 = 0
 OR customer_id IS NULL  OR first_name IS NULL  OR email IS NULL  OR status_code IS NULL         THEN 1 ELSE 0
    END AS dq_null_violation_flag
FROM bronze_customer;

SELECT * FROM vw_bronze_customer_validated WHERE dq_null_violation_flag = 0;
SELECT * FROM vw_bronze_customer_validated WHERE dq_null_violation_flag = 1;

SELECT
    'DQ007_RUN_001' AS dq_run_id,
    'bronze_customer' AS table_name,
    'Customer mandatory field completeness' AS rule_name,
    'Null / Completeness Checks' AS rule_category,
    COUNT(*) AS total_records_checked,
    SUM(CASE WHEN dq_null_violation_flag = 1 THEN 1 ELSE 0 END) AS failed_records_count,
    SUM(CASE WHEN dq_null_violation_flag = 0 THEN 1 ELSE 0 END) AS passed_records_count,
    ROUND(100.0 * SUM(CASE WHEN dq_null_violation_flag = 1 THEN 1 ELSE 0 END) / COUNT(*), 2) AS failure_percentage,
    CURRENT_TIMESTAMP() AS run_timestamp,
    'HIGH' AS severity,
    CASE WHEN SUM(CASE WHEN dq_null_violation_flag = 1 THEN 1 ELSE 0 END) > 0 THEN 'FAILED' ELSE 'PASSED' END AS status
FROM vw_bronze_customer_validated;

