-- ============================================================
-- DQ026 - Order item-to-order reference integrity
-- Reference Integrity Template
-- ============================================================

CREATE OR REPLACE TEMP VIEW bronze_order_item AS
SELECT * FROM VALUES ('OI101','O1001'),('OI102','O999'),('OI103','O1002') AS t(order_item_id, order_id);
CREATE OR REPLACE TEMP VIEW bronze_orders AS
SELECT * FROM VALUES ('O1001'),('O1002') AS t(order_id);

SELECT c.*
FROM bronze_order_item c
INNER JOIN bronze_orders p
    ON c.order_id = p.order_id;

SELECT c.*
FROM bronze_order_item c
LEFT ANTI JOIN bronze_orders p
    ON c.order_id = p.order_id;