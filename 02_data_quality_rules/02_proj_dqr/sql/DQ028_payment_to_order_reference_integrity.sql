-- ============================================================
-- DQ028 - Payment-to-order reference integrity
-- Reference Integrity Template
-- ============================================================

CREATE OR REPLACE TEMP VIEW bronze_payment AS
SELECT * FROM VALUES ('PAY101','O1001'),('PAY102','O999'),('PAY103','O1002') AS t(payment_id, order_id);
CREATE OR REPLACE TEMP VIEW bronze_orders AS
SELECT * FROM VALUES ('O1001'),('O1002') AS t(order_id);

SELECT c.*
FROM bronze_payment c
INNER JOIN bronze_orders p
    ON c.order_id = p.order_id;

SELECT c.*
FROM bronze_payment c
LEFT ANTI JOIN bronze_orders p
    ON c.order_id = p.order_id;