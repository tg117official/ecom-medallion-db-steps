-- ============================================================
-- EXERCISE 10: GOVERNANCE CERTIFICATION SCORECARD
-- Objective:
-- Create a governance readiness control before production release.
--
-- Why this exercise matters:
-- Enterprise governance is not only about building controls.
-- It is about proving that controls are present and validated.
--
-- This table acts as a release gate / checklist.
-- ============================================================

CREATE TABLE IF NOT EXISTS ecomsphere.governance.governance_certification_scorecard (
    dataset_name                 STRING,
    documentation_complete       STRING,   -- Y/N
    owner_assigned               STRING,   -- Y/N
    access_reviewed              STRING,   -- Y/N
    pii_masking_validated        STRING,   -- Y/N/NA
    row_security_validated       STRING,   -- Y/N/NA
    lineage_available            STRING,   -- Y/N
    audit_visibility_available   STRING,   -- Y/N
    schema_approved              STRING,   -- Y/N/NA
    dq_summary_available         STRING,   -- Y/N
    final_status                 STRING,   -- PASS / FAIL
    reviewed_by                  STRING,
    reviewed_ts                  TIMESTAMP,
    review_note                  STRING
)
USING DELTA;

COMMENT ON TABLE ecomsphere.governance.governance_certification_scorecard IS
'Governance release-readiness scorecard for critical datasets';

-- Example scorecard rows
INSERT INTO ecomsphere.governance.governance_certification_scorecard
VALUES
(
    'ecomsphere.gold.fact_sales_order_item',
    'Y',   -- documentation exists
    'Y',   -- owner assigned
    'Y',   -- access reviewed
    'NA',  -- no direct PII column on fact
    'Y',   -- region security tested
    'Y',   -- lineage documented
    'Y',   -- audit visibility available
    'Y',   -- schema dependencies approved
    'Y',   -- dq summary exists
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
);

-- Validation query
SELECT *
FROM ecomsphere.governance.governance_certification_scorecard
ORDER BY reviewed_ts DESC;