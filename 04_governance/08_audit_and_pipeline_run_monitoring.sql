-- ============================================================
-- EXERCISE 8: AUDIT AND PIPELINE RUN MONITORING
-- Objective:
-- Track pipeline runs and prepare for operational governance.
--
-- Why this exercise matters:
-- Governance is incomplete without operational visibility.
-- Teams need to know:
--   what ran,
--   when it ran,
--   whether it succeeded,
--   how many records were processed,
--   and whether errors occurred.
--
-- This script creates a governance audit table for pipeline runs.
-- ============================================================

CREATE TABLE IF NOT EXISTS ecomsphere.governance.pipeline_run_audit (
    run_id              STRING,
    pipeline_name       STRING,
    layer_name          STRING,
    task_name           STRING,
    status              STRING,       -- STARTED / SUCCESS / FAILED
    started_ts          TIMESTAMP,
    ended_ts            TIMESTAMP,
    records_processed   BIGINT,
    error_message       STRING,
    created_ts          TIMESTAMP
)
USING DELTA;

COMMENT ON TABLE ecomsphere.governance.pipeline_run_audit IS
'Stores pipeline execution audit events for governance and operations';

-- Example insert statements for one run
INSERT INTO ecomsphere.governance.pipeline_run_audit
VALUES
('RUN_20260316_001', 'ecom_e2e_pipeline', 'bronze', 'rdbms_to_bronze', 'SUCCESS', current_timestamp(), current_timestamp(), 120000, NULL, current_timestamp()),
('RUN_20260316_001', 'ecom_e2e_pipeline', 'bronze', 'adls_to_bronze', 'SUCCESS', current_timestamp(), current_timestamp(), 45000, NULL, current_timestamp()),
('RUN_20260316_001', 'ecom_e2e_pipeline', 'silver', 'bronze_to_silver', 'SUCCESS', current_timestamp(), current_timestamp(), 98000, NULL, current_timestamp()),
('RUN_20260316_001', 'ecom_e2e_pipeline', 'gold', 'silver_to_gold', 'SUCCESS', current_timestamp(), current_timestamp(), 76000, NULL, current_timestamp());

-- ------------------------------------------------------------
-- If system audit tables are enabled in your Databricks account,
-- you can additionally query those system tables for:
--   user access,
--   privilege changes,
--   object modification events,
--   lineage/audit events.
--
-- This project table remains useful even if system tables exist.
-- ------------------------------------------------------------

-- Validation query
SELECT run_id, pipeline_name, layer_name, task_name, status, records_processed
FROM ecomsphere.governance.pipeline_run_audit
ORDER BY created_ts DESC;