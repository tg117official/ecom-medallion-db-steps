-- ============================================================
-- DQ023 - Duplicate event detection
-- Dedup Template
-- ============================================================


CREATE OR REPLACE TEMP VIEW bronze_order_event_log AS
SELECT * FROM VALUES
('E001','O1001','ORDER',timestamp('2026-03-10 11:00:00')),
('E001','O1001','ORDER',timestamp('2026-03-10 11:05:00')),
('E002','O1002','PAYMENT',timestamp('2026-03-10 11:10:00'))
AS t(event_id, order_id, event_type, event_timestamp);



CREATE OR REPLACE TEMP VIEW vw_bronze_order_event_log_ranked AS
SELECT *,
       ROW_NUMBER() OVER (
           PARTITION BY event_id
           ORDER BY event_timestamp desc, _ingest_ts desc
       ) AS row_num
FROM bronze_order_event_log;

SELECT * FROM vw_bronze_order_event_log_ranked WHERE row_num = 1;
SELECT * FROM vw_bronze_order_event_log_ranked WHERE row_num > 1;

