%sql
-- ============================================================
-- EXERCISE 8: AUDIT AND PIPELINE RUN MONITORING
-- DIRECT PRACTICE VERSION
-- ============================================================
--
-- OBJECTIVE:
-- Learn how to track pipeline runs for governance and operations.
--
-- WHY THIS EXERCISE MATTERS:
-- Governance is incomplete without operational visibility.
-- Teams should be able to answer:
--   1. what ran
--   2. when it ran
--   3. whether it succeeded or failed
--   4. how many records were processed
--   5. what error occurred if something failed
--
-- BEGINNER NOTE:
-- This exercise creates YOUR OWN project-level audit table:
--   ecomsphere.governance.pipeline_run_audit
--
-- This is different from Databricks system tables.
--
-- Your custom audit table:
--   - tracks your project pipeline execution
--   - is fully under your control
--   - is useful for governance reporting in your project
--
-- Databricks system tables:
--   - are platform-provided operational logs
--   - can show account/workspace activity, audit activity, etc.
--   - for example: system.access.audit
--
-- In industry, both are useful together:
--   1. custom pipeline audit table for project-level control
--   2. Databricks system tables for platform-level auditing
--
-- ============================================================


-- ============================================================
-- STEP 0: OPTIONAL CLEANUP FOR PRACTICE
-- ============================================================
-- If you want to rerun the entire exercise cleanly, uncomment
-- the next line.
-- ============================================================

-- DROP TABLE IF EXISTS ecomsphere.governance.pipeline_run_audit;


-- ============================================================
-- STEP 1: CREATE THE PIPELINE AUDIT TABLE
-- ============================================================
-- This table stores one row per pipeline task execution event.
--
-- Example:
--   one run_id may have multiple rows:
--     rdbms_to_bronze
--     adls_to_bronze
--     bronze_to_silver
--     silver_to_gold
--
-- This makes monitoring more detailed.
-- ============================================================

CREATE TABLE IF NOT EXISTS ecomsphere.governance.pipeline_run_audit (
    run_id              STRING,       -- unique pipeline run identifier
    pipeline_name       STRING,       -- e.g. ecom_e2e_pipeline
    layer_name          STRING,       -- bronze / silver / gold / governance
    task_name           STRING,       -- specific task within the run
    status              STRING,       -- STARTED / SUCCESS / FAILED
    started_ts          TIMESTAMP,    -- task start time
    ended_ts            TIMESTAMP,    -- task end time
    records_processed   BIGINT,       -- number of records handled by the task
    error_message       STRING,       -- error details if failed
    created_ts          TIMESTAMP     -- audit row creation time
)
USING DELTA;

COMMENT ON TABLE ecomsphere.governance.pipeline_run_audit IS
'Stores project-level pipeline execution audit events for governance and operations';


-- ============================================================
-- STEP 2: INSERT SAMPLE AUDIT DATA - SUCCESSFUL RUN
-- ============================================================
-- This simulates a pipeline run where all tasks succeeded.
-- ============================================================

INSERT INTO ecomsphere.governance.pipeline_run_audit
VALUES
('RUN_20260316_001', 'ecom_e2e_pipeline', 'bronze', 'rdbms_to_bronze',   'SUCCESS', current_timestamp(), current_timestamp(), 120000, NULL, current_timestamp()),
('RUN_20260316_001', 'ecom_e2e_pipeline', 'bronze', 'adls_to_bronze',    'SUCCESS', current_timestamp(), current_timestamp(),  45000, NULL, current_timestamp()),
('RUN_20260316_001', 'ecom_e2e_pipeline', 'silver', 'bronze_to_silver',  'SUCCESS', current_timestamp(), current_timestamp(),  98000, NULL, current_timestamp()),
('RUN_20260316_001', 'ecom_e2e_pipeline', 'gold',   'silver_to_gold',    'SUCCESS', current_timestamp(), current_timestamp(),  76000, NULL, current_timestamp());


-- ============================================================
-- STEP 3: INSERT SAMPLE AUDIT DATA - FAILED RUN
-- ============================================================
-- This simulates a run where one task failed.
-- This is very important for governance and support analysis.
-- ============================================================

INSERT INTO ecomsphere.governance.pipeline_run_audit
VALUES
('RUN_20260316_002', 'ecom_e2e_pipeline', 'bronze', 'rdbms_to_bronze',   'SUCCESS', current_timestamp(), current_timestamp(), 118500, NULL, current_timestamp()),
('RUN_20260316_002', 'ecom_e2e_pipeline', 'bronze', 'adls_to_bronze',    'SUCCESS', current_timestamp(), current_timestamp(),  47000, NULL, current_timestamp()),
('RUN_20260316_002', 'ecom_e2e_pipeline', 'silver', 'bronze_to_silver',  'FAILED',  current_timestamp(), current_timestamp(),   5200, 'Null value check failed for customer_id in Silver transformation', current_timestamp());


-- ============================================================
-- STEP 4: BASIC VALIDATION QUERY
-- ============================================================
-- This shows all recorded pipeline task events.
-- ============================================================

SELECT
    run_id,
    pipeline_name,
    layer_name,
    task_name,
    status,
    records_processed,
    error_message,
    created_ts
FROM ecomsphere.governance.pipeline_run_audit
ORDER BY created_ts DESC;


-- ============================================================
-- STEP 5: SHOW ONLY FAILED TASKS
-- ============================================================
-- Very common monitoring query:
-- find failed tasks quickly.
-- ============================================================

SELECT
    run_id,
    pipeline_name,
    layer_name,
    task_name,
    status,
    records_processed,
    error_message,
    started_ts,
    ended_ts
FROM ecomsphere.governance.pipeline_run_audit
WHERE status = 'FAILED'
ORDER BY created_ts DESC;


-- ============================================================
-- STEP 6: SUMMARY BY RUN
-- ============================================================
-- This helps you answer:
--   For each run, how many tasks succeeded or failed?
-- ============================================================

SELECT
    run_id,
    pipeline_name,
    COUNT(*) AS total_tasks,
    SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) AS success_tasks,
    SUM(CASE WHEN status = 'FAILED'  THEN 1 ELSE 0 END) AS failed_tasks,
    SUM(records_processed) AS total_records_processed
FROM ecomsphere.governance.pipeline_run_audit
GROUP BY run_id, pipeline_name
ORDER BY run_id;


-- ============================================================
-- STEP 7: SUMMARY BY LAYER
-- ============================================================
-- Helps understand which layer is processing how much data.
-- ============================================================

SELECT
    layer_name,
    COUNT(*) AS task_events,
    SUM(records_processed) AS records_processed_total
FROM ecomsphere.governance.pipeline_run_audit
GROUP BY layer_name
ORDER BY layer_name;


-- ============================================================
-- STEP 8: CURRENT STATUS DASHBOARD STYLE VIEW
-- ============================================================
-- This is useful when you want to quickly see the latest
-- execution events for support/governance review.
-- ============================================================

SELECT
    run_id,
    task_name,
    layer_name,
    status,
    records_processed,
    COALESCE(error_message, 'NO_ERROR') AS error_message
FROM ecomsphere.governance.pipeline_run_audit
ORDER BY created_ts DESC, run_id, layer_name;


-- ============================================================
-- STEP 9: OPTIONAL METADATA REGISTRATION
-- ============================================================
-- If you are maintaining governance.dataset_metadata,
-- you can register this audit table there as well.
-- ============================================================

INSERT INTO ecomsphere.governance.dataset_metadata
VALUES
(
    'table',
    'ecomsphere.governance.pipeline_run_audit',
    'governance',
    'governance',
    'governance_auditors',
    '6 hours',
    'confidential',
    'Project-level pipeline audit log capturing task execution status, record counts, and errors',
    current_timestamp()
);


-- ============================================================
-- STEP 10: OPTIONAL - DEDICATED QUERY FOR THE LATEST FAILED RUN
-- ============================================================
-- Useful when operations team wants the most recent problem.
-- ============================================================

SELECT
    run_id,
    pipeline_name,
    layer_name,
    task_name,
    error_message,
    created_ts
FROM ecomsphere.governance.pipeline_run_audit
WHERE status = 'FAILED'
ORDER BY created_ts DESC
LIMIT 1;


-- ============================================================
-- STEP 11: DATABRICKS SYSTEM TABLE NOTE
-- ============================================================
-- Databricks also provides system tables in the `system` catalog
-- for operational visibility.
--
-- Example:
--   system.access.audit
--
-- That table can help answer questions like:
--   - who accessed which dataset
--   - who changed privileges
--   - what account/workspace activity occurred
--
-- This custom table remains useful because it tracks YOUR
-- project pipeline logic specifically, while system tables
-- track platform/account-level activity.
-- ============================================================

-- Example exploratory query pattern for platform audit logs:
-- SELECT *
-- FROM system.access.audit
-- LIMIT 50;


-- ============================================================
-- STEP 12: NOTES - 5 SCENARIOS WHERE THIS FUNCTIONALITY IS USED
-- ============================================================
--
-- SCENARIO 1:
-- Daily production monitoring
-- When the pipeline runs every 6 hours, the operations team checks
-- pipeline_run_audit to confirm all Bronze, Silver, and Gold tasks
-- finished successfully and processed expected volumes.
--
-- SCENARIO 2:
-- Failure investigation
-- If the Silver transformation fails, the team queries this table
-- to identify which task failed, in which run, and what error
-- message was captured, so debugging becomes faster.
--
-- SCENARIO 3:
-- Governance and compliance review
-- Auditors use this table to verify that expected pipeline runs
-- actually happened and that critical datasets were refreshed
-- according to agreed schedules.
--
-- SCENARIO 4:
-- SLA and freshness tracking
-- Business teams may want to know whether Gold summaries were
-- refreshed on time. This table helps confirm latest run status
-- and whether data was produced within the expected window.
--
-- SCENARIO 5:
-- Volume anomaly detection
-- If records_processed suddenly drops or spikes for a task,
-- this table helps identify unusual behavior, which may indicate
-- source system issues, ingestion problems, or transformation bugs.
--
-- ============================================================