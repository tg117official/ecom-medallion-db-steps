-- ============================================================
-- EXERCISE 7: DATA LINEAGE REGISTRY
-- Objective:
-- Capture and document upstream-to-downstream lineage.
--
-- Why this exercise matters:
-- Industry teams need to answer:
--   "Where did this table come from?"
--   "What downstream objects are impacted if source changes?"
--
-- This exercise stores lineage mappings in a governance table.
-- ============================================================

CREATE TABLE IF NOT EXISTS ecomsphere.governance.dataset_lineage (
    downstream_object   STRING,
    upstream_object     STRING,
    lineage_level       STRING,    -- source_to_bronze / bronze_to_silver / silver_to_gold
    transformation_note STRING,
    captured_ts         TIMESTAMP
)
USING DELTA;

COMMENT ON TABLE ecomsphere.governance.dataset_lineage IS
'Stores logical lineage mappings between datasets across medallion layers';

-- Insert lineage mappings for major project flows
INSERT INTO ecomsphere.governance.dataset_lineage VALUES
('ecomsphere.silver.silver_fact_order_line_enriched', 'ecomsphere.bronze.bronze_orders', 'bronze_to_silver', 'Orders contribute order-level business attributes', current_timestamp()),
('ecomsphere.silver.silver_fact_order_line_enriched', 'ecomsphere.bronze.bronze_order_item', 'bronze_to_silver', 'Order items provide line-level detail', current_timestamp()),
('ecomsphere.silver.silver_fact_order_line_enriched', 'ecomsphere.bronze.bronze_payment', 'bronze_to_silver', 'Payment details enrich payment context', current_timestamp()),
('ecomsphere.gold.fact_sales_order_item', 'ecomsphere.silver.silver_fact_order_line_enriched', 'silver_to_gold', 'Curated order lines transformed into sales fact', current_timestamp()),

('ecomsphere.silver.silver_dim_customer_profile', 'ecomsphere.bronze.bronze_customer', 'bronze_to_silver', 'Customer source contributes identity attributes', current_timestamp()),
('ecomsphere.silver.silver_dim_customer_profile', 'ecomsphere.bronze.bronze_customer_address', 'bronze_to_silver', 'Address source contributes location attributes', current_timestamp()),
('ecomsphere.gold.dim_customer', 'ecomsphere.silver.silver_dim_customer_profile', 'silver_to_gold', 'Conformed customer profile loaded as SCD2 dimension', current_timestamp()),

('ecomsphere.silver.silver_dim_product_master', 'ecomsphere.bronze.bronze_product', 'bronze_to_silver', 'Product source contributes product master attributes', current_timestamp()),
('ecomsphere.silver.silver_dim_product_master', 'ecomsphere.bronze.bronze_product_variant', 'bronze_to_silver', 'Variant source contributes variant attributes', current_timestamp()),
('ecomsphere.silver.silver_dim_product_master', 'ecomsphere.bronze.bronze_product_catalog_feed', 'bronze_to_silver', 'Catalog feed enriches business catalog data', current_timestamp()),
('ecomsphere.gold.dim_product', 'ecomsphere.silver.silver_dim_product_master', 'silver_to_gold', 'Conformed product master loaded as SCD2 dimension', current_timestamp());

-- Validation query: show lineage for a specific Gold table
SELECT *
FROM ecomsphere.governance.dataset_lineage
WHERE downstream_object = 'ecomsphere.gold.fact_sales_order_item'
ORDER BY captured_ts;