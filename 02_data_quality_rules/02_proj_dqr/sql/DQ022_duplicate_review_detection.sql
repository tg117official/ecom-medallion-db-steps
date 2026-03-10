-- ============================================================
-- DQ022 - Duplicate review detection
-- Dedup Template
-- ============================================================


CREATE OR REPLACE TEMP VIEW bronze_customer_review_feed AS
SELECT * FROM VALUES
('R001','C101','PV101',timestamp('2026-03-10 10:00:00'),timestamp('2026-03-10 10:10:00')),
('R001','C101','PV101',timestamp('2026-03-10 10:00:00'),timestamp('2026-03-10 10:20:00')),
('R002','C102','PV102',timestamp('2026-03-10 10:00:00'),timestamp('2026-03-10 10:10:00'))
AS t(review_id, customer_id, product_variant_id, review_created_at, review_updated_at);



CREATE OR REPLACE TEMP VIEW vw_bronze_customer_review_feed_ranked AS
SELECT *,
       ROW_NUMBER() OVER (
           PARTITION BY review_id
           ORDER BY review_updated_at desc, _ingest_ts desc
       ) AS row_num
FROM bronze_customer_review_feed;

SELECT * FROM vw_bronze_customer_review_feed_ranked WHERE row_num = 1;
SELECT * FROM vw_bronze_customer_review_feed_ranked WHERE row_num > 1;

