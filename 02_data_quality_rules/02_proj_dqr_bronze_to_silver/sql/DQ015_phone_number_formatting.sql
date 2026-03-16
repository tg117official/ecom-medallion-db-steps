-- ============================================================
-- DQ015 - Phone number formatting
-- Standardization Template
-- ============================================================


CREATE OR REPLACE TEMP VIEW bronze_customer AS
SELECT * FROM VALUES
('C101',' Amit ',' Patil ',' AMIT@GMAIL.COM ','+91 98765-43210','active'),
('C102','ravi','kumar',' Ravi@Example.Com ','98765 11111','inactive')
AS t(customer_id, first_name, last_name, email, phone_number, status_code);

SELECT *, REGEXP_REPLACE(phone_number, '[^0-9]', '') AS standardized_phone_number
FROM bronze_customer;

