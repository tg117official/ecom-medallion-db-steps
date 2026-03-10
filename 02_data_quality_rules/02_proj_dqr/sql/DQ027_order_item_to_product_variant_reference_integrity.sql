-- ============================================================
-- DQ027 - Order item-to-product variant reference integrity
-- Reference Integrity Template
-- ============================================================

CREATE OR REPLACE TEMP VIEW bronze_order_item AS
SELECT * FROM VALUES ('OI101','PV101'),('OI102','PV999'),('OI103','PV102') AS t(order_item_id, product_variant_id);
CREATE OR REPLACE TEMP VIEW bronze_product_variant AS
SELECT * FROM VALUES ('PV101'),('PV102') AS t(product_variant_id);

SELECT c.*
FROM bronze_order_item c
INNER JOIN bronze_product_variant p
    ON c.product_variant_id = p.product_variant_id;

SELECT c.*
FROM bronze_order_item c
LEFT ANTI JOIN bronze_product_variant p
    ON c.product_variant_id = p.product_variant_id;