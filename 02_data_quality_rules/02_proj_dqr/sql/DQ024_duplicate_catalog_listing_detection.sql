-- ============================================================
-- DQ024 - Duplicate catalog listing detection
-- Dedup Template
-- ============================================================


CREATE OR REPLACE TEMP VIEW bronze_product_catalog_feed AS
SELECT * FROM VALUES
('CR001','PV101','S001',timestamp('2026-03-10 09:00:00')),
('CR002','PV101','S001',timestamp('2026-03-10 09:30:00')),
('CR003','PV102','S002',timestamp('2026-03-10 09:40:00'))
AS t(catalog_record_id, product_variant_id, seller_id, catalog_updated_at);



CREATE OR REPLACE TEMP VIEW vw_bronze_product_catalog_feed_ranked AS
SELECT *,
       ROW_NUMBER() OVER (
           PARTITION BY product_variant_id, seller_id
           ORDER BY catalog_updated_at desc, _ingest_ts desc
       ) AS row_num
FROM bronze_product_catalog_feed;

SELECT * FROM vw_bronze_product_catalog_feed_ranked WHERE row_num = 1;
SELECT * FROM vw_bronze_product_catalog_feed_ranked WHERE row_num > 1;

