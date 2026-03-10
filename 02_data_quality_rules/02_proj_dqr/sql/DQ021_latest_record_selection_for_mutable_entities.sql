-- ============================================================
-- DQ021 - Latest-record selection for mutable entities
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
           PARTITION BY customer_id
           ORDER BY updated_at desc, _ingest_ts desc
       ) AS row_num
FROM bronze_customer;

SELECT * FROM vw_bronze_customer_ranked WHERE row_num = 1;
SELECT * FROM vw_bronze_customer_ranked WHERE row_num > 1;





CREATE OR REPLACE TEMP VIEW vw_bronze_product_ranked AS
SELECT *,
       ROW_NUMBER() OVER (
           PARTITION BY product_id
           ORDER BY updated_at desc, _ingest_ts desc
       ) AS row_num
FROM bronze_product;

SELECT * FROM vw_bronze_product_ranked WHERE row_num = 1;
SELECT * FROM vw_bronze_product_ranked WHERE row_num > 1;





CREATE OR REPLACE TEMP VIEW vw_bronze_product_variant_ranked AS
SELECT *,
       ROW_NUMBER() OVER (
           PARTITION BY product_variant_id
           ORDER BY updated_at desc, _ingest_ts desc
       ) AS row_num
FROM bronze_product_variant;

SELECT * FROM vw_bronze_product_variant_ranked WHERE row_num = 1;
SELECT * FROM vw_bronze_product_variant_ranked WHERE row_num > 1;





CREATE OR REPLACE TEMP VIEW vw_bronze_orders_ranked AS
SELECT *,
       ROW_NUMBER() OVER (
           PARTITION BY order_id
           ORDER BY updated_at desc, _ingest_ts desc
       ) AS row_num
FROM bronze_orders;

SELECT * FROM vw_bronze_orders_ranked WHERE row_num = 1;
SELECT * FROM vw_bronze_orders_ranked WHERE row_num > 1;





CREATE OR REPLACE TEMP VIEW vw_bronze_payment_ranked AS
SELECT *,
       ROW_NUMBER() OVER (
           PARTITION BY payment_id
           ORDER BY updated_at desc, _ingest_ts desc
       ) AS row_num
FROM bronze_payment;

SELECT * FROM vw_bronze_payment_ranked WHERE row_num = 1;
SELECT * FROM vw_bronze_payment_ranked WHERE row_num > 1;





CREATE OR REPLACE TEMP VIEW vw_bronze_shipment_ranked AS
SELECT *,
       ROW_NUMBER() OVER (
           PARTITION BY shipment_id
           ORDER BY updated_at desc, _ingest_ts desc
       ) AS row_num
FROM bronze_shipment;

SELECT * FROM vw_bronze_shipment_ranked WHERE row_num = 1;
SELECT * FROM vw_bronze_shipment_ranked WHERE row_num > 1;

