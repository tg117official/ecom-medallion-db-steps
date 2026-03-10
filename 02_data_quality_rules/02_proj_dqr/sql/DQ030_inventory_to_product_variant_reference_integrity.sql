-- ============================================================
-- DQ030 - Inventory-to-product variant reference integrity
-- Reference Integrity Template
-- ============================================================

CREATE OR REPLACE TEMP VIEW bronze_inventory_level AS
SELECT * FROM VALUES ('W1','PV101'),('W2','PV999'),('W3','PV102') AS t(warehouse_id, product_variant_id);
CREATE OR REPLACE TEMP VIEW bronze_product_variant AS
SELECT * FROM VALUES ('PV101'),('PV102') AS t(product_variant_id);

SELECT c.*
FROM bronze_inventory_level c
INNER JOIN bronze_product_variant p
    ON c.product_variant_id = p.product_variant_id;

SELECT c.*
FROM bronze_inventory_level c
LEFT ANTI JOIN bronze_product_variant p
    ON c.product_variant_id = p.product_variant_id;