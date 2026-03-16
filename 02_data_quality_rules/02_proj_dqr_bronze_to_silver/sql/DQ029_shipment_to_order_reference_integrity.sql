-- ============================================================
-- DQ029 - Shipment-to-order reference integrity
-- Reference Integrity Template
-- ============================================================

CREATE OR REPLACE TEMP VIEW bronze_shipment AS
SELECT * FROM VALUES ('S101','O1001'),('S102','O999'),('S103','O1002') AS t(shipment_id, order_id);
CREATE OR REPLACE TEMP VIEW bronze_orders AS
SELECT * FROM VALUES ('O1001'),('O1002') AS t(order_id);

SELECT c.*
FROM bronze_shipment c
INNER JOIN bronze_orders p
    ON c.order_id = p.order_id;

SELECT c.*
FROM bronze_shipment c
LEFT ANTI JOIN bronze_orders p
    ON c.order_id = p.order_id;