%sql
-- ============================================================
-- EXERCISE 10: GOVERNANCE CERTIFICATION SCORECARD
-- DIRECT PRACTICE VERSION
-- ============================================================
--
-- OBJECTIVE:
-- Create a governance readiness control before production release.
--
-- WHY THIS EXERCISE MATTERS:
-- Enterprise governance is not only about creating controls.
-- It is also about proving that controls are:
--   1. present
--   2. reviewed
--   3. validated
--   4. ready before release
--
-- BEGINNER NOTE:
-- This scorecard acts like a checklist before a dataset is
-- considered ready for governed production usage.
--
-- Think of it like this:
-- Before releasing a dataset, governance asks:
--   - Is documentation available?
--   - Is an owner assigned?
--   - Was access reviewed?
--   - Was masking validated if PII exists?
--   - Was row security tested if needed?
--   - Is lineage available?
--   - Is audit visibility available?
--   - Is schema approved?
--   - Is DQ summary available?
--
-- If enough governance conditions are satisfied,
-- the dataset can be marked PASS.
--
-- ============================================================


-- ============================================================
-- STEP 0: OPTIONAL CLEANUP
-- ============================================================
-- Uncomment if you want to rerun this exercise from scratch.
-- ============================================================

-- DROP TABLE IF EXISTS ecomsphere.governance.governance_certification_scorecard;


-- ============================================================
-- STEP 1: CREATE THE SCORECARD TABLE
-- ============================================================
-- This table stores governance readiness results for datasets.
--
-- One row = one reviewed dataset at one review point in time.
--
-- Column meanings:
--   documentation_complete     -> whether table/column/business docs exist
--   owner_assigned             -> whether responsible owner/steward exists
--   access_reviewed            -> whether access permissions were reviewed
--   pii_masking_validated      -> whether masking was tested where applicable
--   row_security_validated     -> whether row filters were tested where needed
--   lineage_available          -> whether upstream/downstream lineage is documented
--   audit_visibility_available -> whether operational/audit monitoring exists
--   schema_approved            -> whether schema dependencies are approved
--   dq_summary_available       -> whether data quality summary/check exists
--   final_status               -> PASS / FAIL
-- ============================================================

CREATE TABLE IF NOT EXISTS ecomsphere.governance.governance_certification_scorecard (
    dataset_name                 STRING,
    documentation_complete       STRING,   -- Y / N
    owner_assigned               STRING,   -- Y / N
    access_reviewed              STRING,   -- Y / N
    pii_masking_validated        STRING,   -- Y / N / NA
    row_security_validated       STRING,   -- Y / N / NA
    lineage_available            STRING,   -- Y / N
    audit_visibility_available   STRING,   -- Y / N
    schema_approved              STRING,   -- Y / N / NA
    dq_summary_available         STRING,   -- Y / N
    final_status                 STRING,   -- PASS / FAIL
    reviewed_by                  STRING,
    reviewed_ts                  TIMESTAMP,
    review_note                  STRING
)
USING DELTA;

COMMENT ON TABLE ecomsphere.governance.governance_certification_scorecard IS
'Governance release-readiness scorecard for critical datasets';


-- ============================================================
-- STEP 2: INSERT SAMPLE SCORECARD RECORDS
-- ============================================================
-- These rows simulate governance review for critical datasets.
--
-- Notice:
--   NA is used where a control is not relevant.
--
-- Example:
--   fact_sales_order_item may not contain direct PII columns,
--   so pii_masking_validated = NA
-- ============================================================

INSERT INTO ecomsphere.governance.governance_certification_scorecard
VALUES
(
    'ecomsphere.gold.fact_sales_order_item',
    'Y',   -- documentation_complete
    'Y',   -- owner_assigned
    'Y',   -- access_reviewed
    'NA',  -- pii_masking_validated
    'Y',   -- row_security_validated
    'Y',   -- lineage_available
    'Y',   -- audit_visibility_available
    'Y',   -- schema_approved
    'Y',   -- dq_summary_available
    'PASS',
    'governance_auditor_1',
    current_timestamp(),
    'Ready for governed production consumption'
),
(
    'ecomsphere.silver.silver_dim_customer_profile',
    'Y',
    'Y',
    'Y',
    'Y',
    'NA',
    'Y',
    'Y',
    'Y',
    'Y',
    'PASS',
    'governance_auditor_1',
    current_timestamp(),
    'Sensitive customer dataset validated with masking'
),
(
    'ecomsphere.gold.customer_360_document',
    'Y',
    'Y',
    'Y',
    'Y',
    'Y',
    'Y',
    'Y',
    'Y',
    'Y',
    'PASS',
    'governance_auditor_2',
    current_timestamp(),
    'Customer support serving model passed governance review'
),
(
    'ecomsphere.gold.product_360_document',
    'Y',
    'N',
    'Y',
    'NA',
    'NA',
    'Y',
    'Y',
    'Y',
    'Y',
    'FAIL',
    'governance_auditor_2',
    current_timestamp(),
    'Owner assignment missing, release blocked'
),
(
    'ecomsphere.gold.gold_daily_sales_summary',
    'Y',
    'Y',
    'Y',
    'NA',
    'NA',
    'Y',
    'Y',
    'Y',
    'N',
    'FAIL',
    'governance_auditor_1',
    current_timestamp(),
    'DQ summary not yet available, certification failed'
);


-- ============================================================
-- STEP 3: BASIC VALIDATION QUERY
-- ============================================================
-- Shows all review records.
-- ============================================================

SELECT *
FROM ecomsphere.governance.governance_certification_scorecard
ORDER BY reviewed_ts DESC, dataset_name;


-- ============================================================
-- STEP 4: SHOW ONLY FAILED DATASETS
-- ============================================================
-- Useful for governance review meetings because it highlights
-- which datasets are blocked for release.
-- ============================================================

SELECT
    dataset_name,
    final_status,
    reviewed_by,
    reviewed_ts,
    review_note
FROM ecomsphere.governance.governance_certification_scorecard
WHERE final_status = 'FAIL'
ORDER BY reviewed_ts DESC;


-- ============================================================
-- STEP 5: SHOW ONLY PASS DATASETS
-- ============================================================
-- Useful when teams want the approved list of governed datasets.
-- ============================================================

SELECT
    dataset_name,
    final_status,
    reviewed_by,
    reviewed_ts
FROM ecomsphere.governance.governance_certification_scorecard
WHERE final_status = 'PASS'
ORDER BY reviewed_ts DESC;


-- ============================================================
-- STEP 6: CONTROL GAP ANALYSIS
-- ============================================================
-- This query helps identify which control failed for which dataset.
-- It is useful when teams want to know WHAT must be fixed.
-- ============================================================

SELECT
    dataset_name,
    documentation_complete,
    owner_assigned,
    access_reviewed,
    pii_masking_validated,
    row_security_validated,
    lineage_available,
    audit_visibility_available,
    schema_approved,
    dq_summary_available,
    final_status,
    review_note
FROM ecomsphere.governance.governance_certification_scorecard
ORDER BY final_status, dataset_name;


-- ============================================================
-- STEP 7: SIMPLE GOVERNANCE READINESS SUMMARY
-- ============================================================
-- This gives a quick count of how many datasets passed or failed.
-- ============================================================

SELECT
    final_status,
    COUNT(*) AS dataset_count
FROM ecomsphere.governance.governance_certification_scorecard
GROUP BY final_status
ORDER BY final_status;


-- ============================================================
-- STEP 8: OPTIONAL METADATA REGISTRATION
-- ============================================================
-- Register this scorecard table in dataset_metadata if needed.
-- This assumes your governance.dataset_metadata table exists.
-- ============================================================

INSERT INTO ecomsphere.governance.dataset_metadata
VALUES
(
    'table',
    'ecomsphere.governance.governance_certification_scorecard',
    'governance',
    'governance',
    'governance_auditors',
    'on review',
    'confidential',
    'Governance readiness checklist and release gate for critical datasets',
    current_timestamp()
);


-- ============================================================
-- STEP 9: WHAT IS THIS SCORECARD?
-- ============================================================
--
-- This scorecard is a governance control table.
--
-- It does not store business facts like orders or customers.
-- Instead, it stores governance review results for datasets.
--
-- In simple words:
--   it answers:
--   "Is this dataset ready to be released under governance?"
--
-- A PASS means:
--   required governance checks are complete.
--
-- A FAIL means:
--   one or more required governance controls are missing,
--   incomplete, or not yet validated.
--
-- This table is useful before:
--   - production release
--   - UAT signoff
--   - audit review
--   - onboarding a new consumer team
--
-- ============================================================


-- ============================================================
-- STEP 10: NOTES - 5 SCENARIOS WHERE THIS FUNCTIONALITY IS USED
-- ============================================================
--
-- SCENARIO 1:
-- Production release gate
-- Before promoting a Gold dataset to production, the governance
-- team checks this scorecard to ensure all required controls are
-- validated and the dataset is marked PASS.
--
-- SCENARIO 2:
-- Audit preparation
-- During internal or external audits, this table helps prove that
-- critical datasets were reviewed for documentation, access,
-- masking, lineage, and DQ before release.
--
-- SCENARIO 3:
-- New dataset onboarding
-- When a new fact table, dimension, or serving model is introduced,
-- this scorecard helps ensure governance checks are completed
-- before the dataset is shared with analysts or business users.
--
-- SCENARIO 4:
-- Control gap identification
-- If a dataset fails governance review, the team can use this
-- scorecard to identify exactly which control is missing, such as
-- owner assignment, DQ summary, or access review.
--
-- SCENARIO 5:
-- Periodic governance recertification
-- Even after release, datasets may be reviewed again periodically.
-- This scorecard helps track whether existing datasets still meet
-- governance standards after schema changes, access changes, or
-- new business usage.
--
-- ============================================================