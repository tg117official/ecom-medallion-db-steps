-- ============================================================
-- EXERCISE 6: METADATA AND DATA DISCOVERY
-- Objective:
-- Improve discoverability by documenting tables and columns.
--
-- Why this exercise matters:
-- Governance is not only about restriction.
-- It is also about making trusted datasets easy to find,
-- understand, and use correctly.
--
-- Target objects:
--   Silver and Gold tables
-- ============================================================

-- Table comments
COMMENT ON TABLE ecomsphere.silver.silver_dim_customer_profile IS
'Curated conformed customer profile table integrating customer and address details';

COMMENT ON TABLE ecomsphere.silver.silver_dim_product_master IS
'Curated product master integrating OLTP product and file-based catalog details';

COMMENT ON TABLE ecomsphere.silver.silver_fact_order_line_enriched IS
'Validated and enriched order-line dataset before dimensional modeling';

COMMENT ON TABLE ecomsphere.gold.dim_customer IS
'SCD Type 2 customer dimension tracking historical customer attribute changes';

COMMENT ON TABLE ecomsphere.gold.dim_product IS
'SCD Type 2 product dimension tracking historical product attribute changes';

COMMENT ON TABLE ecomsphere.gold.fact_sales_order_item IS
'Order-item grain sales fact table used for downstream analytics and finance reporting';

COMMENT ON TABLE ecomsphere.gold.customer_360_document IS
'Denormalized customer serving model for support and customer-facing use cases';

-- Column comments
COMMENT ON COLUMN ecomsphere.gold.fact_sales_order_item.order_id IS
'Business order identifier coming from the source OLTP system';

COMMENT ON COLUMN ecomsphere.gold.fact_sales_order_item.customer_key IS
'Surrogate key referencing dim_customer';

COMMENT ON COLUMN ecomsphere.gold.fact_sales_order_item.product_key IS
'Surrogate key referencing dim_product';

COMMENT ON COLUMN ecomsphere.gold.fact_sales_order_item.net_amount IS
'Final order-item amount after business validations and enrichment';

-- Insert metadata into governance table
INSERT INTO ecomsphere.governance.dataset_metadata
VALUES
('table', 'ecomsphere.silver.silver_dim_customer_profile', 'customer', 'silver', 'data_engineers', '6 hours', 'pii', 'Curated customer profile table', current_timestamp()),
('table', 'ecomsphere.silver.silver_dim_product_master',   'product',  'silver', 'data_engineers', '6 hours', 'internal', 'Curated product master table', current_timestamp()),
('table', 'ecomsphere.silver.silver_fact_order_line_enriched', 'sales', 'silver', 'data_engineers', '6 hours', 'confidential', 'Business-ready order-line table', current_timestamp()),
('table', 'ecomsphere.gold.dim_customer',                  'customer', 'gold',   'data_analysts',  '6 hours', 'pii', 'Customer dimension with history', current_timestamp()),
('table', 'ecomsphere.gold.dim_product',                   'product',  'gold',   'data_analysts',  '6 hours', 'internal', 'Product dimension with history', current_timestamp()),
('table', 'ecomsphere.gold.fact_sales_order_item',         'sales',    'gold',   'data_analysts',  '6 hours', 'confidential', 'Sales fact at order-item grain', current_timestamp());

-- Validation query
SELECT full_name, business_domain, data_layer, sensitivity_class, description
FROM ecomsphere.governance.dataset_metadata
ORDER BY data_layer, full_name;