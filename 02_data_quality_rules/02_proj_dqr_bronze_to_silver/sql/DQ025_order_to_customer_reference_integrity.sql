-- ============================================================
-- DQ025 - Order-to-customer reference integrity
-- Reference Integrity Template
-- ============================================================

CREATE OR REPLACE TEMP VIEW bronze_orders AS
SELECT * FROM VALUES ('O1001','C101'),('O1002','C999'),('O1003','C102') AS t(order_id, customer_id);
CREATE OR REPLACE TEMP VIEW bronze_customer AS
SELECT * FROM VALUES ('C101'),('C102') AS t(customer_id);

SELECT c.*
FROM bronze_orders c
INNER JOIN bronze_customer p
    ON c.customer_id = p.customer_id;

SELECT c.*
FROM bronze_orders c
LEFT ANTI JOIN bronze_customer p
    ON c.customer_id = p.customer_id;