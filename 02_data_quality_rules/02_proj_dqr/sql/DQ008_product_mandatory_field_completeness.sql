-- ============================================================
-- DQ008 - Product mandatory field completeness
-- Completeness Template
-- ============================================================


CREATE OR REPLACE TEMP VIEW bronze_product AS
SELECT * FROM VALUES
('P101','Shoe','B101','CAT101','desc',timestamp('2026-03-10 09:00:00'),timestamp('2026-03-10 09:10:00'),true,false,timestamp('2026-03-10 09:15:00'),date('2026-03-10')),
('P102',NULL,'B102','CAT102','desc',timestamp('2026-03-10 09:00:00'),timestamp('2026-03-10 09:10:00'),true,false,timestamp('2026-03-10 09:15:00'),date('2026-03-10')),
('P103','Bag',NULL,NULL,'desc',timestamp('2026-03-10 09:00:00'),timestamp('2026-03-10 09:10:00'),true,false,timestamp('2026-03-10 09:15:00'),date('2026-03-10'))
AS t(product_id, product_name, brand_id, default_category_id, short_description, created_at, updated_at, is_active, is_deleted, _ingest_ts, bronze_load_date);


CREATE OR REPLACE TEMP VIEW vw_bronze_product_validated AS
SELECT
    *,
    CONCAT_WS(', '
        , CASE WHEN product_id IS NULL THEN 'product_id' END
        , CASE WHEN product_name IS NULL THEN 'product_name' END
        , CASE WHEN brand_id IS NULL THEN 'brand_id' END
        , CASE WHEN default_category_id IS NULL THEN 'default_category_id' END
    ) AS null_issue,
    CASE
        WHEN 1 = 0
 OR product_id IS NULL  OR product_name IS NULL  OR brand_id IS NULL  OR default_category_id IS NULL         THEN 1 ELSE 0
    END AS dq_null_violation_flag
FROM bronze_product;

SELECT * FROM vw_bronze_product_validated WHERE dq_null_violation_flag = 0;
SELECT * FROM vw_bronze_product_validated WHERE dq_null_violation_flag = 1;

SELECT
    'DQ008_RUN_001' AS dq_run_id,
    'bronze_product' AS table_name,
    'Product mandatory field completeness' AS rule_name,
    'Null / Completeness Checks' AS rule_category,
    COUNT(*) AS total_records_checked,
    SUM(CASE WHEN dq_null_violation_flag = 1 THEN 1 ELSE 0 END) AS failed_records_count,
    SUM(CASE WHEN dq_null_violation_flag = 0 THEN 1 ELSE 0 END) AS passed_records_count,
    ROUND(100.0 * SUM(CASE WHEN dq_null_violation_flag = 1 THEN 1 ELSE 0 END) / COUNT(*), 2) AS failure_percentage,
    CURRENT_TIMESTAMP() AS run_timestamp,
    'HIGH' AS severity,
    CASE WHEN SUM(CASE WHEN dq_null_violation_flag = 1 THEN 1 ELSE 0 END) > 0 THEN 'FAILED' ELSE 'PASSED' END AS status
FROM vw_bronze_product_validated;


CREATE OR REPLACE TEMP VIEW bronze_product_variant AS
SELECT * FROM VALUES
('PV101','P101','SKU101','Red 9',timestamp('2026-03-10 09:00:00'),timestamp('2026-03-10 09:10:00'),true,false,timestamp('2026-03-10 09:15:00'),date('2026-03-10')),
('PV102','P102',NULL,'Blue L',timestamp('2026-03-10 09:00:00'),timestamp('2026-03-10 09:10:00'),true,false,timestamp('2026-03-10 09:15:00'),date('2026-03-10')),
(NULL,'P103','SKU103',NULL,timestamp('2026-03-10 09:00:00'),timestamp('2026-03-10 09:10:00'),true,false,timestamp('2026-03-10 09:15:00'),date('2026-03-10'))
AS t(product_variant_id, product_id, sku, variant_name, created_at, updated_at, is_active, is_deleted, _ingest_ts, bronze_load_date);


CREATE OR REPLACE TEMP VIEW vw_bronze_product_variant_validated AS
SELECT
    *,
    CONCAT_WS(', '
        , CASE WHEN product_variant_id IS NULL THEN 'product_variant_id' END
        , CASE WHEN product_id IS NULL THEN 'product_id' END
        , CASE WHEN sku IS NULL THEN 'sku' END
        , CASE WHEN variant_name IS NULL THEN 'variant_name' END
    ) AS null_issue,
    CASE
        WHEN 1 = 0
 OR product_variant_id IS NULL  OR product_id IS NULL  OR sku IS NULL  OR variant_name IS NULL         THEN 1 ELSE 0
    END AS dq_null_violation_flag
FROM bronze_product_variant;

SELECT * FROM vw_bronze_product_variant_validated WHERE dq_null_violation_flag = 0;
SELECT * FROM vw_bronze_product_variant_validated WHERE dq_null_violation_flag = 1;

SELECT
    'DQ008_RUN_001' AS dq_run_id,
    'bronze_product_variant' AS table_name,
    'Product mandatory field completeness' AS rule_name,
    'Null / Completeness Checks' AS rule_category,
    COUNT(*) AS total_records_checked,
    SUM(CASE WHEN dq_null_violation_flag = 1 THEN 1 ELSE 0 END) AS failed_records_count,
    SUM(CASE WHEN dq_null_violation_flag = 0 THEN 1 ELSE 0 END) AS passed_records_count,
    ROUND(100.0 * SUM(CASE WHEN dq_null_violation_flag = 1 THEN 1 ELSE 0 END) / COUNT(*), 2) AS failure_percentage,
    CURRENT_TIMESTAMP() AS run_timestamp,
    'HIGH' AS severity,
    CASE WHEN SUM(CASE WHEN dq_null_violation_flag = 1 THEN 1 ELSE 0 END) > 0 THEN 'FAILED' ELSE 'PASSED' END AS status
FROM vw_bronze_product_variant_validated;

