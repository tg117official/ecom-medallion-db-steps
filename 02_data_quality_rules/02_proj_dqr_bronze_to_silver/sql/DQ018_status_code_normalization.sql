-- ============================================================
-- DQ018 - Status code normalization
-- Standardization Template
-- ============================================================


CREATE OR REPLACE TEMP VIEW bronze_orders AS
SELECT * FROM VALUES (' sample ') AS t(raw_value);

SELECT *,
       UPPER(TRIM(status_code)) AS normalized_status
FROM bronze_orders;


CREATE OR REPLACE TEMP VIEW bronze_payment AS
SELECT * FROM VALUES (' sample ') AS t(raw_value);

SELECT *,
       UPPER(TRIM(status_code)) AS normalized_status
FROM bronze_payment;


CREATE OR REPLACE TEMP VIEW bronze_shipment AS
SELECT * FROM VALUES (' sample ') AS t(raw_value);

SELECT *,
       UPPER(TRIM(status_code)) AS normalized_status
FROM bronze_shipment;


CREATE OR REPLACE TEMP VIEW bronze_customer_review_feed AS
SELECT * FROM VALUES (' sample ') AS t(raw_value);

SELECT *,
       UPPER(TRIM(status_code)) AS normalized_status
FROM bronze_customer_review_feed;

