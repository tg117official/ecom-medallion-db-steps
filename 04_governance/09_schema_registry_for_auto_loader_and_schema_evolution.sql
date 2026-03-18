%sql
-- ============================================================
-- EXERCISE 9: SCHEMA REGISTRY FOR AUTO LOADER EVOLUTION
-- DIRECT PRACTICE VERSION
-- ============================================================
--
-- OBJECTIVE:
-- Learn how to track schema versions for file-based Bronze
-- ingestion tables using a governance-controlled registry table.
--
-- WHY THIS EXERCISE MATTERS:
-- Auto Loader can detect schema changes automatically, but from
-- a governance point of view we should not rely only on automatic
-- evolution.
--
-- We also need:
--   1. visibility into what changed
--   2. a version history of schema definitions
--   3. approval workflow for changes
--   4. a way to prevent downstream breakage
--
-- ------------------------------------------------------------
-- BEGINNER NOTE: WHAT IS A SCHEMA REGISTRY?
-- ------------------------------------------------------------
-- A schema registry is a table/system that stores schema versions
-- for datasets over time.
--
-- In simple words:
--   it is like a "version history" for table/file structures.
--
-- Example:
-- Version 1:
--   product_id, product_name, category
--
-- Version 2:
--   product_id, product_name, category, brand_name
--
-- Instead of silently accepting every change, we record:
--   - which dataset changed
--   - what the new schema looks like
--   - whether it is approved or still pending
--   - who approved it
--   - why the change happened
--
-- This is especially useful for file-based ingestion because
-- external systems may change JSON structure over time.
--
-- ============================================================


-- ============================================================
-- STEP 0: OPTIONAL CLEANUP
-- ============================================================
-- Uncomment if you want a fresh rerun of this exercise.
-- ============================================================

-- DROP TABLE IF EXISTS ecomsphere.governance.schema_registry;


-- ============================================================
-- STEP 1: CREATE SCHEMA REGISTRY TABLE
-- ============================================================
-- This table stores one row per dataset schema version.
--
-- Important columns:
--   dataset_name    -> which table/feed the schema belongs to
--   version_no      -> schema version number
--   schema_json     -> actual schema structure in JSON form
--   approval_status -> whether this version is approved
--   approved_by     -> who approved it
--   change_note     -> explanation of the change
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
'Tracks schema versions for governed datasets, especially Auto Loader ingested feeds';


-- ============================================================
-- STEP 2: INSERT SAMPLE VERSION HISTORY
-- ============================================================
-- Here we simulate schema evolution for Bronze Auto Loader tables.
--
-- Dataset 1:
--   bronze_product_catalog_feed
--   version 1 -> approved baseline schema
--   version 2 -> pending new column addition
--
-- Dataset 2:
--   bronze_customer_review_feed
--   version 1 -> approved baseline schema
--
-- Dataset 3:
--   bronze_order_event_log
--   version 1 -> approved baseline schema
--   version 2 -> rejected due to problematic change
-- ============================================================

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
    'New optional column brand_name detected from Auto Loader feed'
),
(
    'ecomsphere.bronze.bronze_customer_review_feed',
    1,
    '{"type":"struct","fields":[{"name":"review_id","type":"string"},{"name":"customer_id","type":"string"},{"name":"product_id","type":"string"},{"name":"rating","type":"integer"},{"name":"review_text","type":"string"}]}',
    current_timestamp(),
    'APPROVED',
    'governance_auditor_1',
    'Initial approved review feed schema'
),
(
    'ecomsphere.bronze.bronze_order_event_log',
    1,
    '{"type":"struct","fields":[{"name":"event_id","type":"string"},{"name":"order_id","type":"string"},{"name":"event_type","type":"string"},{"name":"event_ts","type":"timestamp"}]}',
    current_timestamp(),
    'APPROVED',
    'governance_auditor_2',
    'Initial approved order event schema'
),
(
    'ecomsphere.bronze.bronze_order_event_log',
    2,
    '{"type":"struct","fields":[{"name":"event_id","type":"string"},{"name":"order_id","type":"integer"},{"name":"event_type","type":"string"},{"name":"event_ts","type":"timestamp"}]}',
    current_timestamp(),
    'REJECTED',
    'governance_auditor_2',
    'Rejected because order_id datatype changed from string to integer and may break downstream joins'
);


-- ============================================================
-- STEP 3: BASIC VALIDATION QUERY
-- ============================================================
-- Shows all registered schema versions.
-- ============================================================

SELECT
    dataset_name,
    version_no,
    approval_status,
    approved_by,
    change_note,
    effective_ts
FROM ecomsphere.governance.schema_registry
ORDER BY dataset_name, version_no;


-- ============================================================
-- STEP 4: FIND LATEST VERSION PER DATASET
-- ============================================================
-- Very common operational query:
-- what is the latest known schema version for each dataset?
-- ============================================================

SELECT
    dataset_name,
    MAX(version_no) AS latest_version
FROM ecomsphere.governance.schema_registry
GROUP BY dataset_name
ORDER BY dataset_name;


-- ============================================================
-- STEP 5: SHOW ONLY PENDING CHANGES
-- ============================================================
-- Useful for governance review meetings and approval workflows.
-- ============================================================

SELECT
    dataset_name,
    version_no,
    schema_json,
    change_note,
    effective_ts
FROM ecomsphere.governance.schema_registry
WHERE approval_status = 'PENDING'
ORDER BY dataset_name, version_no;


-- ============================================================
-- STEP 6: SHOW ONLY REJECTED CHANGES
-- ============================================================
-- Useful when the team wants to review unsafe changes.
-- ============================================================

SELECT
    dataset_name,
    version_no,
    schema_json,
    approved_by,
    change_note
FROM ecomsphere.governance.schema_registry
WHERE approval_status = 'REJECTED'
ORDER BY dataset_name, version_no;


-- ============================================================
-- STEP 7: OPTIONAL - REGISTER THIS TABLE IN DATASET METADATA
-- ============================================================
-- This assumes governance.dataset_metadata already exists.
-- ============================================================

INSERT INTO ecomsphere.governance.dataset_metadata
VALUES
(
    'table',
    'ecomsphere.governance.schema_registry',
    'governance',
    'governance',
    'governance_auditors',
    'on change',
    'confidential',
    'Registry of schema versions and approval status for governed datasets',
    current_timestamp()
);


-- ============================================================
-- STEP 8: PRACTICAL QUERY - CURRENT APPROVED VERSION ONLY
-- ============================================================
-- This query finds the highest approved version for each dataset.
-- This is often the version considered safe for downstream use.
-- ============================================================

SELECT
    dataset_name,
    MAX(version_no) AS latest_approved_version
FROM ecomsphere.governance.schema_registry
WHERE approval_status = 'APPROVED'
GROUP BY dataset_name
ORDER BY dataset_name;


-- ============================================================
-- STEP 9: NOTES - WHAT IS A SCHEMA REGISTRY?
-- ============================================================
--
-- A schema registry is used to maintain a controlled history of
-- dataset structures over time.
--
-- It is useful because:
--   - schemas can change
--   - downstream pipelines may break
--   - teams need approval before accepting changes
--   - governance needs traceability
--
-- In this project, schema registry is especially relevant for:
--   bronze_product_catalog_feed
--   bronze_customer_review_feed
--   bronze_order_event_log
--
-- because these are file-based ingestion tables where external
-- JSON structure may evolve over time.
--
-- Auto Loader can infer and evolve schema automatically, but a
-- governance registry gives you explicit control and visibility
-- over those changes. 
--
-- ============================================================


-- ============================================================
-- STEP 10: NOTES - 5 SCENARIOS WHERE SCHEMA REGISTRY IS USED
-- ============================================================
--
-- SCENARIO 1:
-- New column arrives in a JSON feed
-- Example: brand_name appears in product catalog feed.
-- Auto Loader detects it, but governance wants that change to be
-- recorded and reviewed before Silver/Gold logic depends on it.
--
-- SCENARIO 2:
-- Datatype changes unexpectedly
-- Example: order_id changes from string to integer.
-- This may break downstream joins, lookups, or DQ rules, so the
-- change should be rejected or carefully reviewed before use.
--
-- SCENARIO 3:
-- Downstream breakage prevention
-- If a Silver transformation expects rating as integer and the
-- source suddenly sends it differently, schema registry helps
-- identify that the schema changed and whether it was approved.
--
-- SCENARIO 4:
-- Audit and compliance traceability
-- Governance teams may ask:
--   who approved this schema version?
--   when did it become effective?
--   why was the change accepted?
-- The registry provides a clear control record.
--
-- SCENARIO 5:
-- Controlled promotion of schema changes
-- A new schema version may first be detected, then marked PENDING,
-- reviewed by governance/data engineering, and only after approval
-- treated as safe for downstream curated and analytical layers.
--
-- ============================================================