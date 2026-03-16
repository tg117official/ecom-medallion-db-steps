-- ============================================================
-- DQ013 - Customer full name derivation
-- Standardization Template
-- ============================================================


CREATE OR REPLACE TEMP VIEW bronze_customer AS
SELECT * FROM VALUES
('C101',' Amit ',' Patil ',' AMIT@GMAIL.COM ','+91 98765-43210','active'),
('C102','ravi','kumar',' Ravi@Example.Com ','98765 11111','inactive')
AS t(customer_id, first_name, last_name, email, phone_number, status_code);

SELECT *, CONCAT_WS(' ', TRIM(first_name), TRIM(last_name)) AS full_name
FROM bronze_customer;

