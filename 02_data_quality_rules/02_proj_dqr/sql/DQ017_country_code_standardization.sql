-- ============================================================
-- DQ017 - Country code standardization
-- Standardization Template
-- ============================================================


CREATE OR REPLACE TEMP VIEW bronze_customer_address AS
SELECT * FROM VALUES
('Nagpur '), (' mumbai'), ('DELHI')
AS t(city);

SELECT *,
       UPPER(TRIM(country_code)) AS standardized_country_code
FROM bronze_customer_address;


CREATE OR REPLACE TEMP VIEW bronze_warehouse AS
SELECT * FROM VALUES
('Nagpur '), (' mumbai'), ('DELHI')
AS t(city);

SELECT *,
       UPPER(TRIM(country_code)) AS standardized_country_code
FROM bronze_warehouse;


CREATE OR REPLACE TEMP VIEW bronze_product_catalog_feed AS
SELECT * FROM VALUES (' sample ') AS t(raw_value);

SELECT *,
       UPPER(TRIM(country_code)) AS standardized_country_code
FROM bronze_product_catalog_feed;

