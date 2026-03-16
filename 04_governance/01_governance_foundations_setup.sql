-- ============================================================
-- EXERCISE 1: GOVERNANCE FOUNDATION SETUP
-- Objective:
-- Create the core Unity Catalog structure for the project.
--
-- Why this exercise matters:
-- Governance becomes difficult if catalog/schema structure is
-- not clearly defined from the beginning.
--
-- In this project, we are using:
--   catalog     : ecomsphere
--   schemas     : bronze, silver, gold, governance
--
-- This exercise creates:
--   1. catalog and schemas
--   2. schema comments
--   3. governance metadata table
-- ============================================================

-- Create catalog
CREATE CATALOG IF NOT EXISTS ecomsphere;
COMMENT ON CATALOG ecomsphere IS
'Governed e-commerce lakehouse catalog for Bronze, Silver, Gold, and Governance layers';

-- Create schemas
CREATE SCHEMA IF NOT EXISTS ecomsphere.bronze;
CREATE SCHEMA IF NOT EXISTS ecomsphere.silver;
CREATE SCHEMA IF NOT EXISTS ecomsphere.gold;
CREATE SCHEMA IF NOT EXISTS ecomsphere.governance;

-- Add schema comments for discovery and documentation
COMMENT ON SCHEMA ecomsphere.bronze IS
'Raw append-only ingestion layer storing minimally transformed source data';

COMMENT ON SCHEMA ecomsphere.silver IS
'Curated conformed layer with validation, deduplication, and business-ready datasets';

COMMENT ON SCHEMA ecomsphere.gold IS
'Analytics consumption layer containing dimensions, facts, summaries, and serving models';

COMMENT ON SCHEMA ecomsphere.governance IS
'Governance layer storing metadata, lineage, schema registry, and pipeline audits';

-- Governance metadata master table
CREATE TABLE IF NOT EXISTS ecomsphere.governance.dataset_metadata (
    object_type           STRING,      -- catalog / schema / table / column
    full_name             STRING,      -- fully qualified name
    business_domain       STRING,      -- customer / product / sales / logistics etc.
    data_layer            STRING,      -- bronze / silver / gold / governance
    owner_group           STRING,      -- responsible team/group
    refresh_frequency     STRING,      -- e.g. 6 hours
    sensitivity_class     STRING,      -- public / internal / confidential / pii
    description           STRING,      -- business definition
    created_ts            TIMESTAMP
)
USING DELTA;

COMMENT ON TABLE ecomsphere.governance.dataset_metadata IS
'Central metadata repository for documenting datasets and governed assets';

-- ============================================================
-- SAMPLE GOVERNANCE METADATA SEED DATA
-- ============================================================
-- Purpose:
-- Populate ecomsphere.governance.dataset_metadata with a richer
-- set of example records across schemas and tables.
--
-- This helps demonstrate:
--   1. schema-level metadata
--   2. table-level metadata
--   3. different business domains
--   4. different sensitivity classes
--   5. different owner groups
--
-- Assumed table structure:
-- CREATE TABLE ecomsphere.governance.dataset_metadata (
--     object_type           STRING,
--     full_name             STRING,
--     business_domain       STRING,
--     data_layer            STRING,
--     owner_group           STRING,
--     refresh_frequency     STRING,
--     sensitivity_class     STRING,
--     description           STRING,
--     created_ts            TIMESTAMP
-- )
-- USING DELTA;
-- ============================================================

INSERT INTO ecomsphere.governance.dataset_metadata
VALUES

-- ============================================================
-- SCHEMA-LEVEL METADATA
-- ============================================================
('schema', 'ecomsphere.bronze',      'platform',   'bronze',     'data_engineers',      '6 hours',  'internal',      'Raw append-only ingestion layer for source-aligned datasets', current_timestamp()),
('schema', 'ecomsphere.silver',      'platform',   'silver',     'data_engineers',      '6 hours',  'internal',      'Curated conformed layer with cleansing, validation, and integration', current_timestamp()),
('schema', 'ecomsphere.gold',        'platform',   'gold',       'data_analysts',       '6 hours',  'internal',      'Analytics, dimensional, summary, and serving layer', current_timestamp()),
('schema', 'ecomsphere.governance',  'platform',   'governance', 'governance_auditors', '6 hours',  'confidential',  'Governance, audit, lineage, metadata, and schema control layer', current_timestamp()),

-- ============================================================
-- BRONZE TABLES
-- ============================================================
('table', 'ecomsphere.bronze.bronze_customer',               'customer',   'bronze', 'data_engineers', '6 hours', 'pii',           'Raw customer master data ingested from RDBMS source with minimal transformation', current_timestamp()),
('table', 'ecomsphere.bronze.bronze_customer_address',       'customer',   'bronze', 'data_engineers', '6 hours', 'pii',           'Raw customer address data from OLTP source preserving source schema', current_timestamp()),
('table', 'ecomsphere.bronze.bronze_product',                'product',    'bronze', 'data_engineers', '6 hours', 'internal',      'Raw product master dataset from source system', current_timestamp()),
('table', 'ecomsphere.bronze.bronze_product_variant',        'product',    'bronze', 'data_engineers', '6 hours', 'internal',      'Raw product variant dataset preserving variant-level source attributes', current_timestamp()),
('table', 'ecomsphere.bronze.bronze_orders',                 'sales',      'bronze', 'data_engineers', '6 hours', 'confidential',  'Raw order header data from OLTP source', current_timestamp()),
('table', 'ecomsphere.bronze.bronze_order_item',             'sales',      'bronze', 'data_engineers', '6 hours', 'confidential',  'Raw order item level data from OLTP source', current_timestamp()),
('table', 'ecomsphere.bronze.bronze_payment',                'finance',    'bronze', 'data_engineers', '6 hours', 'confidential',  'Raw payment transaction data from OLTP source', current_timestamp()),
('table', 'ecomsphere.bronze.bronze_shipment',               'logistics',  'bronze', 'data_engineers', '6 hours', 'internal',      'Raw shipment data including shipment progression and operational status', current_timestamp()),
('table', 'ecomsphere.bronze.bronze_product_catalog_feed',   'product',    'bronze', 'data_engineers', '6 hours', 'internal',      'Raw Auto Loader ingested product catalog JSON feed from ADLS', current_timestamp()),
('table', 'ecomsphere.bronze.bronze_customer_review_feed',   'customer',   'bronze', 'data_engineers', '6 hours', 'internal',      'Raw Auto Loader ingested customer review JSON feed from ADLS', current_timestamp()),
('table', 'ecomsphere.bronze.bronze_order_event_log',        'logistics',  'bronze', 'data_engineers', '6 hours', 'internal',      'Raw Auto Loader ingested order event log JSON feed from ADLS', current_timestamp()),

-- ============================================================
-- SILVER TABLES
-- ============================================================
('table', 'ecomsphere.silver.silver_dim_customer_profile',        'customer',   'silver', 'data_engineers',      '6 hours', 'pii',           'Curated conformed customer profile integrating customer and customer address data', current_timestamp()),
('table', 'ecomsphere.silver.silver_dim_product_master',          'product',    'silver', 'data_engineers',      '6 hours', 'internal',      'Curated product master integrating product, product variant, and catalog feed', current_timestamp()),
('table', 'ecomsphere.silver.silver_fact_order_line_enriched',    'sales',      'silver', 'data_engineers',      '6 hours', 'confidential',  'Validated and enriched order line dataset for downstream dimensional modeling', current_timestamp()),
('table', 'ecomsphere.silver.silver_inventory_snapshot',          'logistics',  'silver', 'data_engineers',      '6 hours', 'internal',      'Curated inventory state by product variant and warehouse', current_timestamp()),
('table', 'ecomsphere.silver.silver_customer_review_curated',     'customer',   'silver', 'data_engineers',      '6 hours', 'internal',      'Cleaned and standardized customer review dataset', current_timestamp()),
('table', 'ecomsphere.silver.silver_order_event_curated',         'logistics',  'silver', 'data_engineers',      '6 hours', 'internal',      'Validated order event timeline data for shipment and tracking analysis', current_timestamp()),
('table', 'ecomsphere.silver.silver_data_quality_audit',          'governance', 'silver', 'governance_auditors', '6 hours', 'confidential',  'Detailed data quality rule execution results across curated datasets', current_timestamp()),

-- ============================================================
-- GOLD TABLES - DIMENSIONS / FACTS / SUMMARIES / SERVING
-- ============================================================
('table', 'ecomsphere.gold.dim_customer',                    'customer',   'gold', 'data_analysts',       '6 hours', 'pii',           'SCD Type 2 customer dimension for historical customer analysis', current_timestamp()),
('table', 'ecomsphere.gold.dim_product',                     'product',    'gold', 'data_analysts',       '6 hours', 'internal',      'SCD Type 2 product dimension for historical product analysis', current_timestamp()),
('table', 'ecomsphere.gold.dim_warehouse',                   'logistics',  'gold', 'data_analysts',       '6 hours', 'internal',      'Warehouse dimension used in fulfillment and inventory analytics', current_timestamp()),
('table', 'ecomsphere.gold.fact_sales_order_item',           'sales',      'gold', 'finance_analysts',    '6 hours', 'confidential',  'Order-item grain sales fact table for financial and operational analytics', current_timestamp()),
('table', 'ecomsphere.gold.fact_inventory_snapshot',         'logistics',  'gold', 'data_analysts',       '6 hours', 'internal',      'Inventory snapshot fact table for stock analysis and planning', current_timestamp()),
('table', 'ecomsphere.gold.fact_customer_review',            'customer',   'gold', 'data_analysts',       '6 hours', 'internal',      'Customer review fact table for sentiment and product feedback analysis', current_timestamp()),
('table', 'ecomsphere.gold.fact_order_event',                'logistics',  'gold', 'data_analysts',       '6 hours', 'internal',      'Order event fact table for shipment journey and tracking analytics', current_timestamp()),
('table', 'ecomsphere.gold.gold_daily_sales_summary',        'sales',      'gold', 'finance_analysts',    '6 hours', 'internal',      'Daily aggregated sales metrics for dashboarding and reporting', current_timestamp()),
('table', 'ecomsphere.gold.gold_product_performance_summary','product',    'gold', 'data_analysts',       '6 hours', 'internal',      'Aggregated product performance metrics including sales and rating trends', current_timestamp()),
('table', 'ecomsphere.gold.gold_customer_360_summary',       'customer',   'gold', 'customer_support',    '6 hours', 'confidential',  'Curated customer summary for support and engagement use cases', current_timestamp()),
('table', 'ecomsphere.gold.gold_order_fulfillment_summary',  'logistics',  'gold', 'data_analysts',       '6 hours', 'internal',      'Aggregated order fulfillment performance metrics', current_timestamp()),
('table', 'ecomsphere.gold.gold_data_quality_summary',       'governance', 'gold', 'governance_auditors', '6 hours', 'confidential',  'Summary-level data quality health indicators for governance reporting', current_timestamp()),
('table', 'ecomsphere.gold.customer_360_document',           'customer',   'gold', 'customer_support',    '6 hours', 'pii',           'Denormalized serving model for customer support use cases', current_timestamp()),
('table', 'ecomsphere.gold.product_360_document',            'product',    'gold', 'data_analysts',       '6 hours', 'internal',      'Denormalized serving model for product-centric consumption', current_timestamp()),
('table', 'ecomsphere.gold.order_tracking_document',         'logistics',  'gold', 'customer_support',    '6 hours', 'internal',      'Serving model for operational order tracking and support workflows', current_timestamp()),

-- ============================================================
-- GOVERNANCE TABLES
-- ============================================================
('table', 'ecomsphere.governance.pipeline_run_audit',        'governance', 'governance', 'governance_auditors', '6 hours', 'confidential', 'Pipeline execution audit log capturing runs, status, and record counts', current_timestamp()),
('table', 'ecomsphere.governance.dataset_metadata',          'governance', 'governance', 'governance_auditors', 'daily',   'confidential', 'Central metadata repository for governed data assets', current_timestamp()),
('table', 'ecomsphere.governance.dataset_lineage',           'governance', 'governance', 'governance_auditors', 'daily',   'confidential', 'Logical lineage registry across Bronze, Silver, and Gold datasets', current_timestamp()),
('table', 'ecomsphere.governance.schema_registry',           'governance', 'governance', 'governance_auditors', 'on change','confidential', 'Schema version tracking table for Auto Loader and controlled evolution', current_timestamp());

-- ============================================================
-- VALIDATION QUERY
-- ============================================================

SELECT *
FROM ecomsphere.governance.dataset_metadata
ORDER BY data_layer, object_type, business_domain, full_name;