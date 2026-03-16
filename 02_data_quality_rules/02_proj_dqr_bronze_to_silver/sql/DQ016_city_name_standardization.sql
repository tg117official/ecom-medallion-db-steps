-- ============================================================
-- DQ016 - City name standardization
-- Standardization Template
-- ============================================================


CREATE OR REPLACE TEMP VIEW bronze_customer_address AS
SELECT * FROM VALUES
('Nagpur '), (' mumbai'), ('DELHI')
AS t(city);

SELECT *, INITCAP(TRIM(city)) AS standardized_city
FROM bronze_customer_address;


CREATE OR REPLACE TEMP VIEW bronze_warehouse AS
SELECT * FROM VALUES
('Nagpur '), (' mumbai'), ('DELHI')
AS t(city);

SELECT *, INITCAP(TRIM(city)) AS standardized_city
FROM bronze_warehouse;


CREATE OR REPLACE TEMP VIEW bronze_order_event_log AS
SELECT * FROM VALUES
('Nagpur '), (' mumbai'), ('DELHI')
AS t(city);

SELECT *, INITCAP(TRIM(city)) AS standardized_city
FROM bronze_order_event_log;

