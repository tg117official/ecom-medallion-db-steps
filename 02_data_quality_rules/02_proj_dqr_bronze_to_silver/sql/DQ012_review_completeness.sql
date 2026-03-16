-- ============================================================
-- DQ012 - Review completeness
-- Completeness Template
-- ============================================================


CREATE OR REPLACE TEMP VIEW bronze_customer_review_feed AS
SELECT * FROM VALUES
('R001','O1001','C101','P101','PV101',5,'Great','Nice','en','positive','approved',true,10,'media1',timestamp('2026-03-10 10:00:00'),timestamp('2026-03-10 10:10:00'),'app',NULL,'f1.json',timestamp('2026-03-10 10:20:00'),date('2026-03-10')),
('R002','O1002','C102','P102','PV102',4,'Good','Text','en','positive','approved',true,3,'media2',NULL,timestamp('2026-03-10 10:10:00'),'app',NULL,'f2.json',timestamp('2026-03-10 10:20:00'),date('2026-03-10')),
(NULL,'O1003',NULL,'P103','PV103',NULL,'Bad','Text','en','negative','pending',false,0,'media3',timestamp('2026-03-10 10:00:00'),timestamp('2026-03-10 10:10:00'),'app',NULL,'f3.json',timestamp('2026-03-10 10:20:00'),date('2026-03-10'))
AS t(review_id, order_id, customer_id, product_id, product_variant_id, rating, review_title, review_text, review_language, review_sentiment, review_status, is_verified_purchase, helpful_votes, media_urls, review_created_at, review_updated_at, source_system, _rescued_data, _source_file, _ingest_ts, bronze_load_date);


CREATE OR REPLACE TEMP VIEW vw_bronze_customer_review_feed_validated AS
SELECT
    *,
    CONCAT_WS(', '
        , CASE WHEN review_id IS NULL THEN 'review_id' END
        , CASE WHEN product_id IS NULL THEN 'product_id' END
        , CASE WHEN customer_id IS NULL THEN 'customer_id' END
        , CASE WHEN rating IS NULL THEN 'rating' END
        , CASE WHEN review_created_at IS NULL THEN 'review_created_at' END
    ) AS null_issue,
    CASE
        WHEN 1 = 0
 OR review_id IS NULL  OR product_id IS NULL  OR customer_id IS NULL  OR rating IS NULL  OR review_created_at IS NULL         THEN 1 ELSE 0
    END AS dq_null_violation_flag
FROM bronze_customer_review_feed;

SELECT * FROM vw_bronze_customer_review_feed_validated WHERE dq_null_violation_flag = 0;
SELECT * FROM vw_bronze_customer_review_feed_validated WHERE dq_null_violation_flag = 1;

SELECT
    'DQ012_RUN_001' AS dq_run_id,
    'bronze_customer_review_feed' AS table_name,
    'Review completeness' AS rule_name,
    'Null / Completeness Checks' AS rule_category,
    COUNT(*) AS total_records_checked,
    SUM(CASE WHEN dq_null_violation_flag = 1 THEN 1 ELSE 0 END) AS failed_records_count,
    SUM(CASE WHEN dq_null_violation_flag = 0 THEN 1 ELSE 0 END) AS passed_records_count,
    ROUND(100.0 * SUM(CASE WHEN dq_null_violation_flag = 1 THEN 1 ELSE 0 END) / COUNT(*), 2) AS failure_percentage,
    CURRENT_TIMESTAMP() AS run_timestamp,
    'HIGH' AS severity,
    CASE WHEN SUM(CASE WHEN dq_null_violation_flag = 1 THEN 1 ELSE 0 END) > 0 THEN 'FAILED' ELSE 'PASSED' END AS status
FROM vw_bronze_customer_review_feed_validated;

