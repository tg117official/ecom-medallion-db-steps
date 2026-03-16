-- ============================================================
-- DQ004 - Unexpected attribute detection in file source
-- Template Type: structural_check
-- ============================================================


CREATE OR REPLACE TEMP VIEW bronze_product_catalog_feed AS
SELECT * FROM VALUES
('CR001','P101','PV101','S001','Nike Shoes',NULL,'catalog_001.json',timestamp('2026-03-10 09:00:00'),date('2026-03-10')),
('CR002','P102','PV102','S002','Adidas Shirt','{"unexpected_field":"abc"}','catalog_002.json',timestamp('2026-03-10 09:10:00'),date('2026-03-10')),
('CR003',NULL,'PV103','S003','Puma Jacket',NULL,'catalog_003.json',NULL,date('2026-03-10'))
AS t(catalog_record_id, product_id, product_variant_id, seller_id, listing_title, _rescued_data, _source_file, _ingest_ts, bronze_load_date);

-- CREATE OR REPLACE TEMP VIEW bronze_product_catalog_feed AS
-- SELECT * FROM ecomsphere.bronze.bronze_product_catalog_feed;

CREATE OR REPLACE TEMP VIEW vw_bronze_product_catalog_feed_validated AS
SELECT
    *,
    CONCAT_WS(', '
            , CASE WHEN _rescued_data IS NULL THEN '_rescued_data' END
            , CASE WHEN _rescued_data IS NOT NULL THEN '_rescued_data_present' END
    ) AS structural_issue,
    CASE
        WHEN
            1 = 0
 OR _rescued_data IS NULL                 OR _rescued_data IS NOT NULL
        THEN 1 ELSE 0
    END AS dq_structural_violation_flag
FROM bronze_product_catalog_feed;

SELECT * FROM vw_bronze_product_catalog_feed_validated WHERE dq_structural_violation_flag = 0;
SELECT * FROM vw_bronze_product_catalog_feed_validated WHERE dq_structural_violation_flag = 1;

SELECT
    'DQ004_RUN_001' AS dq_run_id,
    'bronze_product_catalog_feed' AS table_name,
    'Unexpected attribute detection in file source' AS rule_name,
    'Schema and Structural Checks' AS rule_category,
    COUNT(*) AS total_records_checked,
    SUM(CASE WHEN dq_structural_violation_flag = 1 THEN 1 ELSE 0 END) AS failed_records_count,
    SUM(CASE WHEN dq_structural_violation_flag = 0 THEN 1 ELSE 0 END) AS passed_records_count,
    ROUND(100.0 * SUM(CASE WHEN dq_structural_violation_flag = 1 THEN 1 ELSE 0 END) / COUNT(*), 2) AS failure_percentage,
    CURRENT_TIMESTAMP() AS run_timestamp,
    'MEDIUM' AS severity,
    CASE WHEN SUM(CASE WHEN dq_structural_violation_flag = 1 THEN 1 ELSE 0 END) > 0 THEN 'FAILED' ELSE 'PASSED' END AS status
FROM vw_bronze_product_catalog_feed_validated;


CREATE OR REPLACE TEMP VIEW bronze_customer_review_feed AS
SELECT * FROM VALUES
('R001','O1001','C101','P101',5,NULL,'review_001.json',timestamp('2026-03-10 10:00:00'),date('2026-03-10')),
('R002','O1002','C102','P102',4,'{"bad_json":"x"}','review_002.json',timestamp('2026-03-10 10:10:00'),date('2026-03-10')),
(NULL,'O1003','C103','P103',3,NULL,'review_003.json',timestamp('2026-03-10 10:20:00'),NULL)
AS t(review_id, order_id, customer_id, product_id, rating, _rescued_data, _source_file, _ingest_ts, bronze_load_date);

-- CREATE OR REPLACE TEMP VIEW bronze_customer_review_feed AS
-- SELECT * FROM ecomsphere.bronze.bronze_customer_review_feed;

CREATE OR REPLACE TEMP VIEW vw_bronze_customer_review_feed_validated AS
SELECT
    *,
    CONCAT_WS(', '
            , CASE WHEN _rescued_data IS NULL THEN '_rescued_data' END
            , CASE WHEN _rescued_data IS NOT NULL THEN '_rescued_data_present' END
    ) AS structural_issue,
    CASE
        WHEN
            1 = 0
 OR _rescued_data IS NULL                 OR _rescued_data IS NOT NULL
        THEN 1 ELSE 0
    END AS dq_structural_violation_flag
FROM bronze_customer_review_feed;

SELECT * FROM vw_bronze_customer_review_feed_validated WHERE dq_structural_violation_flag = 0;
SELECT * FROM vw_bronze_customer_review_feed_validated WHERE dq_structural_violation_flag = 1;

SELECT
    'DQ004_RUN_001' AS dq_run_id,
    'bronze_customer_review_feed' AS table_name,
    'Unexpected attribute detection in file source' AS rule_name,
    'Schema and Structural Checks' AS rule_category,
    COUNT(*) AS total_records_checked,
    SUM(CASE WHEN dq_structural_violation_flag = 1 THEN 1 ELSE 0 END) AS failed_records_count,
    SUM(CASE WHEN dq_structural_violation_flag = 0 THEN 1 ELSE 0 END) AS passed_records_count,
    ROUND(100.0 * SUM(CASE WHEN dq_structural_violation_flag = 1 THEN 1 ELSE 0 END) / COUNT(*), 2) AS failure_percentage,
    CURRENT_TIMESTAMP() AS run_timestamp,
    'MEDIUM' AS severity,
    CASE WHEN SUM(CASE WHEN dq_structural_violation_flag = 1 THEN 1 ELSE 0 END) > 0 THEN 'FAILED' ELSE 'PASSED' END AS status
FROM vw_bronze_customer_review_feed_validated;


CREATE OR REPLACE TEMP VIEW bronze_order_event_log AS
SELECT * FROM VALUES
('E001','O1001','ORDER',timestamp('2026-03-10 11:00:00'),NULL,'event_001.json',timestamp('2026-03-10 11:05:00'),date('2026-03-10')),
('E002','O1002','PAYMENT',timestamp('2026-03-10 11:10:00'),'{"bad_field":"123"}','event_002.json',timestamp('2026-03-10 11:11:00'),date('2026-03-10')),
('E003','O1003',NULL,timestamp('2026-03-10 11:20:00'),NULL,'event_003.json',NULL,date('2026-03-10'))
AS t(event_id, order_id, event_type, event_timestamp, _rescued_data, _source_file, _ingest_ts, bronze_load_date);

-- CREATE OR REPLACE TEMP VIEW bronze_order_event_log AS
-- SELECT * FROM ecomsphere.bronze.bronze_order_event_log;

CREATE OR REPLACE TEMP VIEW vw_bronze_order_event_log_validated AS
SELECT
    *,
    CONCAT_WS(', '
            , CASE WHEN _rescued_data IS NULL THEN '_rescued_data' END
            , CASE WHEN _rescued_data IS NOT NULL THEN '_rescued_data_present' END
    ) AS structural_issue,
    CASE
        WHEN
            1 = 0
 OR _rescued_data IS NULL                 OR _rescued_data IS NOT NULL
        THEN 1 ELSE 0
    END AS dq_structural_violation_flag
FROM bronze_order_event_log;

SELECT * FROM vw_bronze_order_event_log_validated WHERE dq_structural_violation_flag = 0;
SELECT * FROM vw_bronze_order_event_log_validated WHERE dq_structural_violation_flag = 1;

SELECT
    'DQ004_RUN_001' AS dq_run_id,
    'bronze_order_event_log' AS table_name,
    'Unexpected attribute detection in file source' AS rule_name,
    'Schema and Structural Checks' AS rule_category,
    COUNT(*) AS total_records_checked,
    SUM(CASE WHEN dq_structural_violation_flag = 1 THEN 1 ELSE 0 END) AS failed_records_count,
    SUM(CASE WHEN dq_structural_violation_flag = 0 THEN 1 ELSE 0 END) AS passed_records_count,
    ROUND(100.0 * SUM(CASE WHEN dq_structural_violation_flag = 1 THEN 1 ELSE 0 END) / COUNT(*), 2) AS failure_percentage,
    CURRENT_TIMESTAMP() AS run_timestamp,
    'MEDIUM' AS severity,
    CASE WHEN SUM(CASE WHEN dq_structural_violation_flag = 1 THEN 1 ELSE 0 END) > 0 THEN 'FAILED' ELSE 'PASSED' END AS status
FROM vw_bronze_order_event_log_validated;

