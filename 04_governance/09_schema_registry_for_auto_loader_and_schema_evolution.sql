-- ============================================================
-- EXERCISE 9: SCHEMA REGISTRY FOR AUTO LOADER EVOLUTION
-- Objective:
-- Track schema versions for file-based Bronze ingestion tables.
--
-- Why this exercise matters:
-- Auto Loader can detect schema evolution, but governance requires
-- visibility and controlled approval of schema changes.
--
-- Target Bronze tables:
--   bronze_product_catalog_feed
--   bronze_customer_review_feed
--   bronze_order_event_log
-- ============================================================

CREATE TABLE IF NOT EXISTS ecomsphere.governance.schema_registry (
    dataset_name        STRING,
    version_no          INT,
    schema_json         STRING,
    effective_ts        TIMESTAMP,
    approval_status     STRING,      -- PENDING / APPROVED / REJECTED
    approved_by         STRING,
    change_note         STRING
)
USING DELTA;

COMMENT ON TABLE ecomsphere.governance.schema_registry IS
'Tracks schema versions for governed datasets, especially Auto Loader feeds';

-- Example inserts
INSERT INTO ecomsphere.governance.schema_registry
VALUES
(
    'ecomsphere.bronze.bronze_product_catalog_feed',
    1,
    '{"type":"struct","fields":[{"name":"product_id","type":"string"},{"name":"product_name","type":"string"},{"name":"category","type":"string"}]}',
    current_timestamp(),
    'APPROVED',
    'governance_auditor_1',
    'Initial approved schema version'
),
(
    'ecomsphere.bronze.bronze_product_catalog_feed',
    2,
    '{"type":"struct","fields":[{"name":"product_id","type":"string"},{"name":"product_name","type":"string"},{"name":"category","type":"string"},{"name":"brand_name","type":"string"}]}',
    current_timestamp(),
    'PENDING',
    NULL,
    'New column detected from Auto Loader feed'
);

-- This table can be joined with pipeline controls later so that
-- schema evolution does not silently break Silver/Gold logic.

-- Validation query
SELECT dataset_name, version_no, approval_status, approved_by, change_note
FROM ecomsphere.governance.schema_registry
ORDER BY dataset_name, version_no;