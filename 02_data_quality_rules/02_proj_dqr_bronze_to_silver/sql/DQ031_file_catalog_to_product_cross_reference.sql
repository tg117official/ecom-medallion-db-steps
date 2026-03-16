-- ============================================================
-- DQ031 - File catalog-to-product cross-reference
-- Reference Integrity Template
-- ============================================================

CREATE OR REPLACE TEMP VIEW bronze_product_catalog_feed AS
SELECT * FROM VALUES ('CR001','P101','PV101'),('CR002','P999','PV999'),('CR003','P102','PV102') AS t(catalog_record_id, product_id, product_variant_id);
CREATE OR REPLACE TEMP VIEW bronze_product_variant AS
SELECT * FROM VALUES ('PV101'),('PV102') AS t(product_variant_id);

SELECT c.*
FROM bronze_product_catalog_feed c
INNER JOIN bronze_product_variant p
    ON c.product_variant_id = p.product_variant_id;

SELECT c.*
FROM bronze_product_catalog_feed c
LEFT ANTI JOIN bronze_product_variant p
    ON c.product_variant_id = p.product_variant_id;