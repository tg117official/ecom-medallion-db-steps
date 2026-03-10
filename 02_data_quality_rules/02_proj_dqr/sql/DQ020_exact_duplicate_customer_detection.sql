-- ============================================================
-- DQ020 - Exact duplicate customer detection
-- Dedup Template
-- ============================================================


CREATE OR REPLACE TEMP VIEW bronze_customer AS
SELECT * FROM VALUES
('C101','Amit','amit@gmail.com',timestamp('2026-03-10 09:00:00'),timestamp('2026-03-10 09:05:00')),
('C101','Amit','amit@gmail.com',timestamp('2026-03-10 09:00:00'),timestamp('2026-03-10 09:10:00')),
('C102','Ravi','ravi@gmail.com',timestamp('2026-03-10 09:00:00'),timestamp('2026-03-10 09:05:00'))
AS t(customer_id, first_name, email, created_at, updated_at);



CREATE OR REPLACE TEMP VIEW vw_bronze_customer_ranked AS
SELECT *,
       ROW_NUMBER() OVER (
           PARTITION BY customer_id, first_name, last_name, email, phone_number, status_code
           ORDER BY updated_at desc, _ingest_ts desc
       ) AS row_num
FROM bronze_customer;

SELECT * FROM vw_bronze_customer_ranked WHERE row_num = 1;
SELECT * FROM vw_bronze_customer_ranked WHERE row_num > 1;

