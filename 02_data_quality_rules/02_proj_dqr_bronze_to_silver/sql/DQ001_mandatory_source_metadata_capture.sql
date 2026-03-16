-- ============================================================
-- DQ001 - Mandatory source metadata capture
-- Template Type: structural_check
-- ============================================================


CREATE OR REPLACE TEMP VIEW bronze_brand AS
SELECT * FROM VALUES
('ID001', timestamp('2026-03-10 09:00:00'), date('2026-03-10')),
('ID002', NULL, date('2026-03-10')),
('ID003', timestamp('2026-03-10 09:20:00'), NULL)
AS t(record_id, _ingest_ts, bronze_load_date);

-- CREATE OR REPLACE TEMP VIEW bronze_brand AS
-- SELECT * FROM ecomsphere.bronze.bronze_brand;

CREATE OR REPLACE TEMP VIEW vw_bronze_brand_validated AS
SELECT
    *,
    CONCAT_WS(', '
            , CASE WHEN _ingest_ts IS NULL THEN '_ingest_ts' END
            , CASE WHEN bronze_load_date IS NULL THEN 'bronze_load_date' END
    ) AS structural_issue,
    CASE
        WHEN
            1 = 0
 OR _ingest_ts IS NULL  OR bronze_load_date IS NULL         THEN 1 ELSE 0
    END AS dq_structural_violation_flag
FROM bronze_brand;

SELECT * FROM vw_bronze_brand_validated WHERE dq_structural_violation_flag = 0;
SELECT * FROM vw_bronze_brand_validated WHERE dq_structural_violation_flag = 1;

SELECT
    'DQ001_RUN_001' AS dq_run_id,
    'bronze_brand' AS table_name,
    'Mandatory source metadata capture' AS rule_name,
    'Schema and Structural Checks' AS rule_category,
    COUNT(*) AS total_records_checked,
    SUM(CASE WHEN dq_structural_violation_flag = 1 THEN 1 ELSE 0 END) AS failed_records_count,
    SUM(CASE WHEN dq_structural_violation_flag = 0 THEN 1 ELSE 0 END) AS passed_records_count,
    ROUND(100.0 * SUM(CASE WHEN dq_structural_violation_flag = 1 THEN 1 ELSE 0 END) / COUNT(*), 2) AS failure_percentage,
    CURRENT_TIMESTAMP() AS run_timestamp,
    'HIGH' AS severity,
    CASE WHEN SUM(CASE WHEN dq_structural_violation_flag = 1 THEN 1 ELSE 0 END) > 0 THEN 'FAILED' ELSE 'PASSED' END AS status
FROM vw_bronze_brand_validated;


CREATE OR REPLACE TEMP VIEW bronze_category AS
SELECT * FROM VALUES
('ID001', timestamp('2026-03-10 09:00:00'), date('2026-03-10')),
('ID002', NULL, date('2026-03-10')),
('ID003', timestamp('2026-03-10 09:20:00'), NULL)
AS t(record_id, _ingest_ts, bronze_load_date);

-- CREATE OR REPLACE TEMP VIEW bronze_category AS
-- SELECT * FROM ecomsphere.bronze.bronze_category;

CREATE OR REPLACE TEMP VIEW vw_bronze_category_validated AS
SELECT
    *,
    CONCAT_WS(', '
            , CASE WHEN _ingest_ts IS NULL THEN '_ingest_ts' END
            , CASE WHEN bronze_load_date IS NULL THEN 'bronze_load_date' END
    ) AS structural_issue,
    CASE
        WHEN
            1 = 0
 OR _ingest_ts IS NULL  OR bronze_load_date IS NULL         THEN 1 ELSE 0
    END AS dq_structural_violation_flag
FROM bronze_category;

SELECT * FROM vw_bronze_category_validated WHERE dq_structural_violation_flag = 0;
SELECT * FROM vw_bronze_category_validated WHERE dq_structural_violation_flag = 1;

SELECT
    'DQ001_RUN_001' AS dq_run_id,
    'bronze_category' AS table_name,
    'Mandatory source metadata capture' AS rule_name,
    'Schema and Structural Checks' AS rule_category,
    COUNT(*) AS total_records_checked,
    SUM(CASE WHEN dq_structural_violation_flag = 1 THEN 1 ELSE 0 END) AS failed_records_count,
    SUM(CASE WHEN dq_structural_violation_flag = 0 THEN 1 ELSE 0 END) AS passed_records_count,
    ROUND(100.0 * SUM(CASE WHEN dq_structural_violation_flag = 1 THEN 1 ELSE 0 END) / COUNT(*), 2) AS failure_percentage,
    CURRENT_TIMESTAMP() AS run_timestamp,
    'HIGH' AS severity,
    CASE WHEN SUM(CASE WHEN dq_structural_violation_flag = 1 THEN 1 ELSE 0 END) > 0 THEN 'FAILED' ELSE 'PASSED' END AS status
FROM vw_bronze_category_validated;


CREATE OR REPLACE TEMP VIEW bronze_warehouse AS
SELECT * FROM VALUES
('ID001', timestamp('2026-03-10 09:00:00'), date('2026-03-10')),
('ID002', NULL, date('2026-03-10')),
('ID003', timestamp('2026-03-10 09:20:00'), NULL)
AS t(record_id, _ingest_ts, bronze_load_date);

-- CREATE OR REPLACE TEMP VIEW bronze_warehouse AS
-- SELECT * FROM ecomsphere.bronze.bronze_warehouse;

CREATE OR REPLACE TEMP VIEW vw_bronze_warehouse_validated AS
SELECT
    *,
    CONCAT_WS(', '
            , CASE WHEN _ingest_ts IS NULL THEN '_ingest_ts' END
            , CASE WHEN bronze_load_date IS NULL THEN 'bronze_load_date' END
    ) AS structural_issue,
    CASE
        WHEN
            1 = 0
 OR _ingest_ts IS NULL  OR bronze_load_date IS NULL         THEN 1 ELSE 0
    END AS dq_structural_violation_flag
FROM bronze_warehouse;

SELECT * FROM vw_bronze_warehouse_validated WHERE dq_structural_violation_flag = 0;
SELECT * FROM vw_bronze_warehouse_validated WHERE dq_structural_violation_flag = 1;

SELECT
    'DQ001_RUN_001' AS dq_run_id,
    'bronze_warehouse' AS table_name,
    'Mandatory source metadata capture' AS rule_name,
    'Schema and Structural Checks' AS rule_category,
    COUNT(*) AS total_records_checked,
    SUM(CASE WHEN dq_structural_violation_flag = 1 THEN 1 ELSE 0 END) AS failed_records_count,
    SUM(CASE WHEN dq_structural_violation_flag = 0 THEN 1 ELSE 0 END) AS passed_records_count,
    ROUND(100.0 * SUM(CASE WHEN dq_structural_violation_flag = 1 THEN 1 ELSE 0 END) / COUNT(*), 2) AS failure_percentage,
    CURRENT_TIMESTAMP() AS run_timestamp,
    'HIGH' AS severity,
    CASE WHEN SUM(CASE WHEN dq_structural_violation_flag = 1 THEN 1 ELSE 0 END) > 0 THEN 'FAILED' ELSE 'PASSED' END AS status
FROM vw_bronze_warehouse_validated;


CREATE OR REPLACE TEMP VIEW bronze_customer AS
SELECT * FROM VALUES
('ID001', timestamp('2026-03-10 09:00:00'), date('2026-03-10')),
('ID002', NULL, date('2026-03-10')),
('ID003', timestamp('2026-03-10 09:20:00'), NULL)
AS t(record_id, _ingest_ts, bronze_load_date);

-- CREATE OR REPLACE TEMP VIEW bronze_customer AS
-- SELECT * FROM ecomsphere.bronze.bronze_customer;

CREATE OR REPLACE TEMP VIEW vw_bronze_customer_validated AS
SELECT
    *,
    CONCAT_WS(', '
            , CASE WHEN _ingest_ts IS NULL THEN '_ingest_ts' END
            , CASE WHEN bronze_load_date IS NULL THEN 'bronze_load_date' END
    ) AS structural_issue,
    CASE
        WHEN
            1 = 0
 OR _ingest_ts IS NULL  OR bronze_load_date IS NULL         THEN 1 ELSE 0
    END AS dq_structural_violation_flag
FROM bronze_customer;

SELECT * FROM vw_bronze_customer_validated WHERE dq_structural_violation_flag = 0;
SELECT * FROM vw_bronze_customer_validated WHERE dq_structural_violation_flag = 1;

SELECT
    'DQ001_RUN_001' AS dq_run_id,
    'bronze_customer' AS table_name,
    'Mandatory source metadata capture' AS rule_name,
    'Schema and Structural Checks' AS rule_category,
    COUNT(*) AS total_records_checked,
    SUM(CASE WHEN dq_structural_violation_flag = 1 THEN 1 ELSE 0 END) AS failed_records_count,
    SUM(CASE WHEN dq_structural_violation_flag = 0 THEN 1 ELSE 0 END) AS passed_records_count,
    ROUND(100.0 * SUM(CASE WHEN dq_structural_violation_flag = 1 THEN 1 ELSE 0 END) / COUNT(*), 2) AS failure_percentage,
    CURRENT_TIMESTAMP() AS run_timestamp,
    'HIGH' AS severity,
    CASE WHEN SUM(CASE WHEN dq_structural_violation_flag = 1 THEN 1 ELSE 0 END) > 0 THEN 'FAILED' ELSE 'PASSED' END AS status
FROM vw_bronze_customer_validated;


CREATE OR REPLACE TEMP VIEW bronze_customer_address AS
SELECT * FROM VALUES
('ID001', timestamp('2026-03-10 09:00:00'), date('2026-03-10')),
('ID002', NULL, date('2026-03-10')),
('ID003', timestamp('2026-03-10 09:20:00'), NULL)
AS t(record_id, _ingest_ts, bronze_load_date);

-- CREATE OR REPLACE TEMP VIEW bronze_customer_address AS
-- SELECT * FROM ecomsphere.bronze.bronze_customer_address;

CREATE OR REPLACE TEMP VIEW vw_bronze_customer_address_validated AS
SELECT
    *,
    CONCAT_WS(', '
            , CASE WHEN _ingest_ts IS NULL THEN '_ingest_ts' END
            , CASE WHEN bronze_load_date IS NULL THEN 'bronze_load_date' END
    ) AS structural_issue,
    CASE
        WHEN
            1 = 0
 OR _ingest_ts IS NULL  OR bronze_load_date IS NULL         THEN 1 ELSE 0
    END AS dq_structural_violation_flag
FROM bronze_customer_address;

SELECT * FROM vw_bronze_customer_address_validated WHERE dq_structural_violation_flag = 0;
SELECT * FROM vw_bronze_customer_address_validated WHERE dq_structural_violation_flag = 1;

SELECT
    'DQ001_RUN_001' AS dq_run_id,
    'bronze_customer_address' AS table_name,
    'Mandatory source metadata capture' AS rule_name,
    'Schema and Structural Checks' AS rule_category,
    COUNT(*) AS total_records_checked,
    SUM(CASE WHEN dq_structural_violation_flag = 1 THEN 1 ELSE 0 END) AS failed_records_count,
    SUM(CASE WHEN dq_structural_violation_flag = 0 THEN 1 ELSE 0 END) AS passed_records_count,
    ROUND(100.0 * SUM(CASE WHEN dq_structural_violation_flag = 1 THEN 1 ELSE 0 END) / COUNT(*), 2) AS failure_percentage,
    CURRENT_TIMESTAMP() AS run_timestamp,
    'HIGH' AS severity,
    CASE WHEN SUM(CASE WHEN dq_structural_violation_flag = 1 THEN 1 ELSE 0 END) > 0 THEN 'FAILED' ELSE 'PASSED' END AS status
FROM vw_bronze_customer_address_validated;


CREATE OR REPLACE TEMP VIEW bronze_product AS
SELECT * FROM VALUES
('ID001', timestamp('2026-03-10 09:00:00'), date('2026-03-10')),
('ID002', NULL, date('2026-03-10')),
('ID003', timestamp('2026-03-10 09:20:00'), NULL)
AS t(record_id, _ingest_ts, bronze_load_date);

-- CREATE OR REPLACE TEMP VIEW bronze_product AS
-- SELECT * FROM ecomsphere.bronze.bronze_product;

CREATE OR REPLACE TEMP VIEW vw_bronze_product_validated AS
SELECT
    *,
    CONCAT_WS(', '
            , CASE WHEN _ingest_ts IS NULL THEN '_ingest_ts' END
            , CASE WHEN bronze_load_date IS NULL THEN 'bronze_load_date' END
    ) AS structural_issue,
    CASE
        WHEN
            1 = 0
 OR _ingest_ts IS NULL  OR bronze_load_date IS NULL         THEN 1 ELSE 0
    END AS dq_structural_violation_flag
FROM bronze_product;

SELECT * FROM vw_bronze_product_validated WHERE dq_structural_violation_flag = 0;
SELECT * FROM vw_bronze_product_validated WHERE dq_structural_violation_flag = 1;

SELECT
    'DQ001_RUN_001' AS dq_run_id,
    'bronze_product' AS table_name,
    'Mandatory source metadata capture' AS rule_name,
    'Schema and Structural Checks' AS rule_category,
    COUNT(*) AS total_records_checked,
    SUM(CASE WHEN dq_structural_violation_flag = 1 THEN 1 ELSE 0 END) AS failed_records_count,
    SUM(CASE WHEN dq_structural_violation_flag = 0 THEN 1 ELSE 0 END) AS passed_records_count,
    ROUND(100.0 * SUM(CASE WHEN dq_structural_violation_flag = 1 THEN 1 ELSE 0 END) / COUNT(*), 2) AS failure_percentage,
    CURRENT_TIMESTAMP() AS run_timestamp,
    'HIGH' AS severity,
    CASE WHEN SUM(CASE WHEN dq_structural_violation_flag = 1 THEN 1 ELSE 0 END) > 0 THEN 'FAILED' ELSE 'PASSED' END AS status
FROM vw_bronze_product_validated;


CREATE OR REPLACE TEMP VIEW bronze_product_variant AS
SELECT * FROM VALUES
('ID001', timestamp('2026-03-10 09:00:00'), date('2026-03-10')),
('ID002', NULL, date('2026-03-10')),
('ID003', timestamp('2026-03-10 09:20:00'), NULL)
AS t(record_id, _ingest_ts, bronze_load_date);

-- CREATE OR REPLACE TEMP VIEW bronze_product_variant AS
-- SELECT * FROM ecomsphere.bronze.bronze_product_variant;

CREATE OR REPLACE TEMP VIEW vw_bronze_product_variant_validated AS
SELECT
    *,
    CONCAT_WS(', '
            , CASE WHEN _ingest_ts IS NULL THEN '_ingest_ts' END
            , CASE WHEN bronze_load_date IS NULL THEN 'bronze_load_date' END
    ) AS structural_issue,
    CASE
        WHEN
            1 = 0
 OR _ingest_ts IS NULL  OR bronze_load_date IS NULL         THEN 1 ELSE 0
    END AS dq_structural_violation_flag
FROM bronze_product_variant;

SELECT * FROM vw_bronze_product_variant_validated WHERE dq_structural_violation_flag = 0;
SELECT * FROM vw_bronze_product_variant_validated WHERE dq_structural_violation_flag = 1;

SELECT
    'DQ001_RUN_001' AS dq_run_id,
    'bronze_product_variant' AS table_name,
    'Mandatory source metadata capture' AS rule_name,
    'Schema and Structural Checks' AS rule_category,
    COUNT(*) AS total_records_checked,
    SUM(CASE WHEN dq_structural_violation_flag = 1 THEN 1 ELSE 0 END) AS failed_records_count,
    SUM(CASE WHEN dq_structural_violation_flag = 0 THEN 1 ELSE 0 END) AS passed_records_count,
    ROUND(100.0 * SUM(CASE WHEN dq_structural_violation_flag = 1 THEN 1 ELSE 0 END) / COUNT(*), 2) AS failure_percentage,
    CURRENT_TIMESTAMP() AS run_timestamp,
    'HIGH' AS severity,
    CASE WHEN SUM(CASE WHEN dq_structural_violation_flag = 1 THEN 1 ELSE 0 END) > 0 THEN 'FAILED' ELSE 'PASSED' END AS status
FROM vw_bronze_product_variant_validated;


CREATE OR REPLACE TEMP VIEW bronze_inventory_level AS
SELECT * FROM VALUES
('ID001', timestamp('2026-03-10 09:00:00'), date('2026-03-10')),
('ID002', NULL, date('2026-03-10')),
('ID003', timestamp('2026-03-10 09:20:00'), NULL)
AS t(record_id, _ingest_ts, bronze_load_date);

-- CREATE OR REPLACE TEMP VIEW bronze_inventory_level AS
-- SELECT * FROM ecomsphere.bronze.bronze_inventory_level;

CREATE OR REPLACE TEMP VIEW vw_bronze_inventory_level_validated AS
SELECT
    *,
    CONCAT_WS(', '
            , CASE WHEN _ingest_ts IS NULL THEN '_ingest_ts' END
            , CASE WHEN bronze_load_date IS NULL THEN 'bronze_load_date' END
    ) AS structural_issue,
    CASE
        WHEN
            1 = 0
 OR _ingest_ts IS NULL  OR bronze_load_date IS NULL         THEN 1 ELSE 0
    END AS dq_structural_violation_flag
FROM bronze_inventory_level;

SELECT * FROM vw_bronze_inventory_level_validated WHERE dq_structural_violation_flag = 0;
SELECT * FROM vw_bronze_inventory_level_validated WHERE dq_structural_violation_flag = 1;

SELECT
    'DQ001_RUN_001' AS dq_run_id,
    'bronze_inventory_level' AS table_name,
    'Mandatory source metadata capture' AS rule_name,
    'Schema and Structural Checks' AS rule_category,
    COUNT(*) AS total_records_checked,
    SUM(CASE WHEN dq_structural_violation_flag = 1 THEN 1 ELSE 0 END) AS failed_records_count,
    SUM(CASE WHEN dq_structural_violation_flag = 0 THEN 1 ELSE 0 END) AS passed_records_count,
    ROUND(100.0 * SUM(CASE WHEN dq_structural_violation_flag = 1 THEN 1 ELSE 0 END) / COUNT(*), 2) AS failure_percentage,
    CURRENT_TIMESTAMP() AS run_timestamp,
    'HIGH' AS severity,
    CASE WHEN SUM(CASE WHEN dq_structural_violation_flag = 1 THEN 1 ELSE 0 END) > 0 THEN 'FAILED' ELSE 'PASSED' END AS status
FROM vw_bronze_inventory_level_validated;


CREATE OR REPLACE TEMP VIEW bronze_orders AS
SELECT * FROM VALUES
('ID001', timestamp('2026-03-10 09:00:00'), date('2026-03-10')),
('ID002', NULL, date('2026-03-10')),
('ID003', timestamp('2026-03-10 09:20:00'), NULL)
AS t(record_id, _ingest_ts, bronze_load_date);

-- CREATE OR REPLACE TEMP VIEW bronze_orders AS
-- SELECT * FROM ecomsphere.bronze.bronze_orders;

CREATE OR REPLACE TEMP VIEW vw_bronze_orders_validated AS
SELECT
    *,
    CONCAT_WS(', '
            , CASE WHEN _ingest_ts IS NULL THEN '_ingest_ts' END
            , CASE WHEN bronze_load_date IS NULL THEN 'bronze_load_date' END
    ) AS structural_issue,
    CASE
        WHEN
            1 = 0
 OR _ingest_ts IS NULL  OR bronze_load_date IS NULL         THEN 1 ELSE 0
    END AS dq_structural_violation_flag
FROM bronze_orders;

SELECT * FROM vw_bronze_orders_validated WHERE dq_structural_violation_flag = 0;
SELECT * FROM vw_bronze_orders_validated WHERE dq_structural_violation_flag = 1;

SELECT
    'DQ001_RUN_001' AS dq_run_id,
    'bronze_orders' AS table_name,
    'Mandatory source metadata capture' AS rule_name,
    'Schema and Structural Checks' AS rule_category,
    COUNT(*) AS total_records_checked,
    SUM(CASE WHEN dq_structural_violation_flag = 1 THEN 1 ELSE 0 END) AS failed_records_count,
    SUM(CASE WHEN dq_structural_violation_flag = 0 THEN 1 ELSE 0 END) AS passed_records_count,
    ROUND(100.0 * SUM(CASE WHEN dq_structural_violation_flag = 1 THEN 1 ELSE 0 END) / COUNT(*), 2) AS failure_percentage,
    CURRENT_TIMESTAMP() AS run_timestamp,
    'HIGH' AS severity,
    CASE WHEN SUM(CASE WHEN dq_structural_violation_flag = 1 THEN 1 ELSE 0 END) > 0 THEN 'FAILED' ELSE 'PASSED' END AS status
FROM vw_bronze_orders_validated;


CREATE OR REPLACE TEMP VIEW bronze_order_item AS
SELECT * FROM VALUES
('ID001', timestamp('2026-03-10 09:00:00'), date('2026-03-10')),
('ID002', NULL, date('2026-03-10')),
('ID003', timestamp('2026-03-10 09:20:00'), NULL)
AS t(record_id, _ingest_ts, bronze_load_date);

-- CREATE OR REPLACE TEMP VIEW bronze_order_item AS
-- SELECT * FROM ecomsphere.bronze.bronze_order_item;

CREATE OR REPLACE TEMP VIEW vw_bronze_order_item_validated AS
SELECT
    *,
    CONCAT_WS(', '
            , CASE WHEN _ingest_ts IS NULL THEN '_ingest_ts' END
            , CASE WHEN bronze_load_date IS NULL THEN 'bronze_load_date' END
    ) AS structural_issue,
    CASE
        WHEN
            1 = 0
 OR _ingest_ts IS NULL  OR bronze_load_date IS NULL         THEN 1 ELSE 0
    END AS dq_structural_violation_flag
FROM bronze_order_item;

SELECT * FROM vw_bronze_order_item_validated WHERE dq_structural_violation_flag = 0;
SELECT * FROM vw_bronze_order_item_validated WHERE dq_structural_violation_flag = 1;

SELECT
    'DQ001_RUN_001' AS dq_run_id,
    'bronze_order_item' AS table_name,
    'Mandatory source metadata capture' AS rule_name,
    'Schema and Structural Checks' AS rule_category,
    COUNT(*) AS total_records_checked,
    SUM(CASE WHEN dq_structural_violation_flag = 1 THEN 1 ELSE 0 END) AS failed_records_count,
    SUM(CASE WHEN dq_structural_violation_flag = 0 THEN 1 ELSE 0 END) AS passed_records_count,
    ROUND(100.0 * SUM(CASE WHEN dq_structural_violation_flag = 1 THEN 1 ELSE 0 END) / COUNT(*), 2) AS failure_percentage,
    CURRENT_TIMESTAMP() AS run_timestamp,
    'HIGH' AS severity,
    CASE WHEN SUM(CASE WHEN dq_structural_violation_flag = 1 THEN 1 ELSE 0 END) > 0 THEN 'FAILED' ELSE 'PASSED' END AS status
FROM vw_bronze_order_item_validated;


CREATE OR REPLACE TEMP VIEW bronze_payment AS
SELECT * FROM VALUES
('ID001', timestamp('2026-03-10 09:00:00'), date('2026-03-10')),
('ID002', NULL, date('2026-03-10')),
('ID003', timestamp('2026-03-10 09:20:00'), NULL)
AS t(record_id, _ingest_ts, bronze_load_date);

-- CREATE OR REPLACE TEMP VIEW bronze_payment AS
-- SELECT * FROM ecomsphere.bronze.bronze_payment;

CREATE OR REPLACE TEMP VIEW vw_bronze_payment_validated AS
SELECT
    *,
    CONCAT_WS(', '
            , CASE WHEN _ingest_ts IS NULL THEN '_ingest_ts' END
            , CASE WHEN bronze_load_date IS NULL THEN 'bronze_load_date' END
    ) AS structural_issue,
    CASE
        WHEN
            1 = 0
 OR _ingest_ts IS NULL  OR bronze_load_date IS NULL         THEN 1 ELSE 0
    END AS dq_structural_violation_flag
FROM bronze_payment;

SELECT * FROM vw_bronze_payment_validated WHERE dq_structural_violation_flag = 0;
SELECT * FROM vw_bronze_payment_validated WHERE dq_structural_violation_flag = 1;

SELECT
    'DQ001_RUN_001' AS dq_run_id,
    'bronze_payment' AS table_name,
    'Mandatory source metadata capture' AS rule_name,
    'Schema and Structural Checks' AS rule_category,
    COUNT(*) AS total_records_checked,
    SUM(CASE WHEN dq_structural_violation_flag = 1 THEN 1 ELSE 0 END) AS failed_records_count,
    SUM(CASE WHEN dq_structural_violation_flag = 0 THEN 1 ELSE 0 END) AS passed_records_count,
    ROUND(100.0 * SUM(CASE WHEN dq_structural_violation_flag = 1 THEN 1 ELSE 0 END) / COUNT(*), 2) AS failure_percentage,
    CURRENT_TIMESTAMP() AS run_timestamp,
    'HIGH' AS severity,
    CASE WHEN SUM(CASE WHEN dq_structural_violation_flag = 1 THEN 1 ELSE 0 END) > 0 THEN 'FAILED' ELSE 'PASSED' END AS status
FROM vw_bronze_payment_validated;


CREATE OR REPLACE TEMP VIEW bronze_shipment AS
SELECT * FROM VALUES
('ID001', timestamp('2026-03-10 09:00:00'), date('2026-03-10')),
('ID002', NULL, date('2026-03-10')),
('ID003', timestamp('2026-03-10 09:20:00'), NULL)
AS t(record_id, _ingest_ts, bronze_load_date);

-- CREATE OR REPLACE TEMP VIEW bronze_shipment AS
-- SELECT * FROM ecomsphere.bronze.bronze_shipment;

CREATE OR REPLACE TEMP VIEW vw_bronze_shipment_validated AS
SELECT
    *,
    CONCAT_WS(', '
            , CASE WHEN _ingest_ts IS NULL THEN '_ingest_ts' END
            , CASE WHEN bronze_load_date IS NULL THEN 'bronze_load_date' END
    ) AS structural_issue,
    CASE
        WHEN
            1 = 0
 OR _ingest_ts IS NULL  OR bronze_load_date IS NULL         THEN 1 ELSE 0
    END AS dq_structural_violation_flag
FROM bronze_shipment;

SELECT * FROM vw_bronze_shipment_validated WHERE dq_structural_violation_flag = 0;
SELECT * FROM vw_bronze_shipment_validated WHERE dq_structural_violation_flag = 1;

SELECT
    'DQ001_RUN_001' AS dq_run_id,
    'bronze_shipment' AS table_name,
    'Mandatory source metadata capture' AS rule_name,
    'Schema and Structural Checks' AS rule_category,
    COUNT(*) AS total_records_checked,
    SUM(CASE WHEN dq_structural_violation_flag = 1 THEN 1 ELSE 0 END) AS failed_records_count,
    SUM(CASE WHEN dq_structural_violation_flag = 0 THEN 1 ELSE 0 END) AS passed_records_count,
    ROUND(100.0 * SUM(CASE WHEN dq_structural_violation_flag = 1 THEN 1 ELSE 0 END) / COUNT(*), 2) AS failure_percentage,
    CURRENT_TIMESTAMP() AS run_timestamp,
    'HIGH' AS severity,
    CASE WHEN SUM(CASE WHEN dq_structural_violation_flag = 1 THEN 1 ELSE 0 END) > 0 THEN 'FAILED' ELSE 'PASSED' END AS status
FROM vw_bronze_shipment_validated;


CREATE OR REPLACE TEMP VIEW bronze_product_catalog_feed AS
SELECT * FROM VALUES
('CR001','P101','PV101','S001','Nike Shoes',NULL,'catalog_001.json',timestamp('2026-03-10 09:00:00'),date('2026-03-10')),
('CR002','P102','PV102','S002','Adidas Shirt','{"unexpected_field":"abc"}','catalog_002.json',timestamp('2026-03-10 09:10:00'),date('2026-03-10')),
('CR003',NULL,'PV103','S003','Puma Jacket',NULL,'catalog_003.json',NULL,date('2026-03-10'))
AS t(catalog_record_id, product_id, product_variant_id, seller_id, listing_title, _rescued_data, _source_file, _ingest_ts, bronze_load_date);

-- CREATE OR REPLACE TEMP VIEW bronze_product_catalog_feed AS
-- SELECT * FROM ecomsphere.bronze.bronze_product_catalog_feed;

CREATE OR REPLACE TEMP VIEW vw_bronze_product_catalog_feed_validated AS
SELECT
    *,
    CONCAT_WS(', '
            , CASE WHEN _ingest_ts IS NULL THEN '_ingest_ts' END
            , CASE WHEN bronze_load_date IS NULL THEN 'bronze_load_date' END
            , CASE WHEN _source_file IS NULL THEN '_source_file' END
            , CASE WHEN _rescued_data IS NOT NULL THEN '_rescued_data_present' END
    ) AS structural_issue,
    CASE
        WHEN
            1 = 0
 OR _ingest_ts IS NULL  OR bronze_load_date IS NULL  OR _source_file IS NULL                 OR _rescued_data IS NOT NULL
        THEN 1 ELSE 0
    END AS dq_structural_violation_flag
FROM bronze_product_catalog_feed;

SELECT * FROM vw_bronze_product_catalog_feed_validated WHERE dq_structural_violation_flag = 0;
SELECT * FROM vw_bronze_product_catalog_feed_validated WHERE dq_structural_violation_flag = 1;

SELECT
    'DQ001_RUN_001' AS dq_run_id,
    'bronze_product_catalog_feed' AS table_name,
    'Mandatory source metadata capture' AS rule_name,
    'Schema and Structural Checks' AS rule_category,
    COUNT(*) AS total_records_checked,
    SUM(CASE WHEN dq_structural_violation_flag = 1 THEN 1 ELSE 0 END) AS failed_records_count,
    SUM(CASE WHEN dq_structural_violation_flag = 0 THEN 1 ELSE 0 END) AS passed_records_count,
    ROUND(100.0 * SUM(CASE WHEN dq_structural_violation_flag = 1 THEN 1 ELSE 0 END) / COUNT(*), 2) AS failure_percentage,
    CURRENT_TIMESTAMP() AS run_timestamp,
    'HIGH' AS severity,
    CASE WHEN SUM(CASE WHEN dq_structural_violation_flag = 1 THEN 1 ELSE 0 END) > 0 THEN 'FAILED' ELSE 'PASSED' END AS status
FROM vw_bronze_product_catalog_feed_validated;


CREATE OR REPLACE TEMP VIEW bronze_customer_review_feed AS
SELECT * FROM VALUES
('R001','O1001','C101','P101',5,NULL,'review_001.json',timestamp('2026-03-10 10:00:00'),date('2026-03-10')),
('R002','O1002','C102','P102',4,'{"bad_json":"x"}','review_002.json',timestamp('2026-03-10 10:10:00'),date('2026-03-10')),
(NULL,'O1003','C103','P103',3,NULL,'review_003.json',timestamp('2026-03-10 10:20:00'),NULL)
AS t(review_id, order_id, customer_id, product_id, rating, _rescued_data, _source_file, _ingest_ts, bronze_load_date);

-- CREATE OR REPLACE TEMP VIEW bronze_customer_review_feed AS
-- SELECT * FROM ecomsphere.bronze.bronze_customer_review_feed;

CREATE OR REPLACE TEMP VIEW vw_bronze_customer_review_feed_validated AS
SELECT
    *,
    CONCAT_WS(', '
            , CASE WHEN _ingest_ts IS NULL THEN '_ingest_ts' END
            , CASE WHEN bronze_load_date IS NULL THEN 'bronze_load_date' END
            , CASE WHEN _source_file IS NULL THEN '_source_file' END
            , CASE WHEN _rescued_data IS NOT NULL THEN '_rescued_data_present' END
    ) AS structural_issue,
    CASE
        WHEN
            1 = 0
 OR _ingest_ts IS NULL  OR bronze_load_date IS NULL  OR _source_file IS NULL                 OR _rescued_data IS NOT NULL
        THEN 1 ELSE 0
    END AS dq_structural_violation_flag
FROM bronze_customer_review_feed;

SELECT * FROM vw_bronze_customer_review_feed_validated WHERE dq_structural_violation_flag = 0;
SELECT * FROM vw_bronze_customer_review_feed_validated WHERE dq_structural_violation_flag = 1;

SELECT
    'DQ001_RUN_001' AS dq_run_id,
    'bronze_customer_review_feed' AS table_name,
    'Mandatory source metadata capture' AS rule_name,
    'Schema and Structural Checks' AS rule_category,
    COUNT(*) AS total_records_checked,
    SUM(CASE WHEN dq_structural_violation_flag = 1 THEN 1 ELSE 0 END) AS failed_records_count,
    SUM(CASE WHEN dq_structural_violation_flag = 0 THEN 1 ELSE 0 END) AS passed_records_count,
    ROUND(100.0 * SUM(CASE WHEN dq_structural_violation_flag = 1 THEN 1 ELSE 0 END) / COUNT(*), 2) AS failure_percentage,
    CURRENT_TIMESTAMP() AS run_timestamp,
    'HIGH' AS severity,
    CASE WHEN SUM(CASE WHEN dq_structural_violation_flag = 1 THEN 1 ELSE 0 END) > 0 THEN 'FAILED' ELSE 'PASSED' END AS status
FROM vw_bronze_customer_review_feed_validated;


CREATE OR REPLACE TEMP VIEW bronze_order_event_log AS
SELECT * FROM VALUES
('E001','O1001','ORDER',timestamp('2026-03-10 11:00:00'),NULL,'event_001.json',timestamp('2026-03-10 11:05:00'),date('2026-03-10')),
('E002','O1002','PAYMENT',timestamp('2026-03-10 11:10:00'),'{"bad_field":"123"}','event_002.json',timestamp('2026-03-10 11:11:00'),date('2026-03-10')),
('E003','O1003',NULL,timestamp('2026-03-10 11:20:00'),NULL,'event_003.json',NULL,date('2026-03-10'))
AS t(event_id, order_id, event_type, event_timestamp, _rescued_data, _source_file, _ingest_ts, bronze_load_date);

-- CREATE OR REPLACE TEMP VIEW bronze_order_event_log AS
-- SELECT * FROM ecomsphere.bronze.bronze_order_event_log;

CREATE OR REPLACE TEMP VIEW vw_bronze_order_event_log_validated AS
SELECT
    *,
    CONCAT_WS(', '
            , CASE WHEN _ingest_ts IS NULL THEN '_ingest_ts' END
            , CASE WHEN bronze_load_date IS NULL THEN 'bronze_load_date' END
            , CASE WHEN _source_file IS NULL THEN '_source_file' END
            , CASE WHEN _rescued_data IS NOT NULL THEN '_rescued_data_present' END
    ) AS structural_issue,
    CASE
        WHEN
            1 = 0
 OR _ingest_ts IS NULL  OR bronze_load_date IS NULL  OR _source_file IS NULL                 OR _rescued_data IS NOT NULL
        THEN 1 ELSE 0
    END AS dq_structural_violation_flag
FROM bronze_order_event_log;

SELECT * FROM vw_bronze_order_event_log_validated WHERE dq_structural_violation_flag = 0;
SELECT * FROM vw_bronze_order_event_log_validated WHERE dq_structural_violation_flag = 1;

SELECT
    'DQ001_RUN_001' AS dq_run_id,
    'bronze_order_event_log' AS table_name,
    'Mandatory source metadata capture' AS rule_name,
    'Schema and Structural Checks' AS rule_category,
    COUNT(*) AS total_records_checked,
    SUM(CASE WHEN dq_structural_violation_flag = 1 THEN 1 ELSE 0 END) AS failed_records_count,
    SUM(CASE WHEN dq_structural_violation_flag = 0 THEN 1 ELSE 0 END) AS passed_records_count,
    ROUND(100.0 * SUM(CASE WHEN dq_structural_violation_flag = 1 THEN 1 ELSE 0 END) / COUNT(*), 2) AS failure_percentage,
    CURRENT_TIMESTAMP() AS run_timestamp,
    'HIGH' AS severity,
    CASE WHEN SUM(CASE WHEN dq_structural_violation_flag = 1 THEN 1 ELSE 0 END) > 0 THEN 'FAILED' ELSE 'PASSED' END AS status
FROM vw_bronze_order_event_log_validated;

