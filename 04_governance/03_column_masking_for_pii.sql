-- ============================================================
-- EXERCISE 3: COLUMN MASKING FOR PII (DIRECT PRACTICE VERSION)
-- ============================================================
--
-- Objective:
-- Practice Unity Catalog column masking end-to-end using a
-- small sample customer profile table.
--
-- What this script does:
--   1. Creates a practice table in ecomsphere.silver
--   2. Inserts sample customer rows
--   3. Creates masking functions in ecomsphere.governance
--   4. Applies masks using ALTER TABLE ... ALTER COLUMN ... SET MASK
--   5. Validates results
--
-- Important prerequisites:
--   - Table must be in Unity Catalog
--   - Column masks are Unity Catalog only
--   - Feature applies in Databricks SQL / DBR 12.2 LTS+
--
-- Assumed groups already exist:
--   governance_auditors
--   customer_support
--   data_analysts
--
-- Access logic in this exercise:
--   governance_auditors -> see original values
--   customer_support    -> see original values
--   everyone else       -> see masked values
--
-- ============================================================


-- ============================================================
-- STEP 0: OPTIONAL CLEANUP
-- ============================================================
-- Drop old practice objects if they already exist.
-- This makes the script easy to rerun while learning.
-- ============================================================

DROP TABLE IF EXISTS ecomsphere.silver.silver_dim_customer_profile_masking_demo;

-- If you want to recreate functions freshly every time,
-- you can keep CREATE OR REPLACE below, so explicit DROP is optional.


-- ============================================================
-- STEP 1: CREATE A PRACTICE TABLE
-- ============================================================
-- We are creating a small demo table instead of depending on
-- your real silver_dim_customer_profile table.
--
-- This makes the exercise safe, isolated, and easy to test.
-- ============================================================

CREATE TABLE IF NOT EXISTS ecomsphere.silver.silver_dim_customer_profile_masking_demo (
    customer_id      STRING,
    customer_name    STRING,
    email            STRING,
    phone_number     STRING,
    address          STRING,
    city             STRING,
    country          STRING,
    created_ts       TIMESTAMP
)
USING DELTA;

COMMENT ON TABLE ecomsphere.silver.silver_dim_customer_profile_masking_demo IS
'Practice table for learning Unity Catalog column masking on customer PII fields';

COMMENT ON COLUMN ecomsphere.silver.silver_dim_customer_profile_masking_demo.email IS
'Customer email address - sensitive PII';

COMMENT ON COLUMN ecomsphere.silver.silver_dim_customer_profile_masking_demo.phone_number IS
'Customer phone number - sensitive PII';

COMMENT ON COLUMN ecomsphere.silver.silver_dim_customer_profile_masking_demo.address IS
'Customer address - sensitive PII';


-- ============================================================
-- STEP 2: INSERT SAMPLE DATA
-- ============================================================
-- These rows help you immediately test masking behavior.
-- ============================================================

INSERT INTO ecomsphere.silver.silver_dim_customer_profile_masking_demo
VALUES
('C001', 'Aarav Sharma',   'aarav.sharma@example.com',   '9876543210', '101 Lake View Road, Pune',      'Pune',      'India', current_timestamp()),
('C002', 'Meera Iyer',     'meera.iyer@example.com',     '9123456780', '22 Green Park Avenue, Chennai', 'Chennai',   'India', current_timestamp()),
('C003', 'Rohan Verma',    'rohan.verma@example.com',    '9988776655', '7 Hill Street, Mumbai',         'Mumbai',    'India', current_timestamp()),
('C004', 'Sneha Kulkarni', 'sneha.kulkarni@example.com', '9012345678', '45 River Side, Nagpur',         'Nagpur',    'India', current_timestamp());


-- ============================================================
-- STEP 3: PRE-MASK VALIDATION
-- ============================================================
-- Run this before applying masks.
-- At this stage, all users with SELECT access will see original
-- values because no mask is attached yet.
-- ============================================================

SELECT *
FROM ecomsphere.silver.silver_dim_customer_profile_masking_demo
ORDER BY customer_id;


-- ============================================================
-- STEP 4: CREATE MASKING FUNCTIONS
-- ============================================================
-- A column mask is a SQL UDF.
--
-- Rule:
--   The first parameter maps to the masked column value.
--   The function returns the value that should be shown.
--
-- We are using is_account_group_member(...) so the result depends
-- on who is querying the table.
-- ============================================================

CREATE OR REPLACE FUNCTION ecomsphere.governance.mask_email(input_email STRING)
RETURNS STRING
COMMENT 'Masks email for non-privileged users'
RETURN
  CASE
    WHEN is_account_group_member('governance_auditors') THEN input_email
    WHEN is_account_group_member('customer_support') THEN input_email
    ELSE regexp_replace(input_email, '(^.).*(@.*$)', '$1***$2')
  END;

-- Explanation:
--   aarav.sharma@example.com
-- becomes something like:
--   a***@example.com
--
-- governance_auditors and customer_support see full email.
-- Others see masked email.

CREATE OR REPLACE FUNCTION ecomsphere.governance.mask_phone(input_phone STRING)
RETURNS STRING
COMMENT 'Masks phone number for non-privileged users'
RETURN
  CASE
    WHEN is_account_group_member('governance_auditors') THEN input_phone
    WHEN is_account_group_member('customer_support') THEN input_phone
    ELSE concat('XXXXXX', right(input_phone, 4))
  END;

-- Explanation:
--   9876543210
-- becomes:
--   XXXXXX3210

CREATE OR REPLACE FUNCTION ecomsphere.governance.mask_address(input_address STRING)
RETURNS STRING
COMMENT 'Masks address for non-privileged users'
RETURN
  CASE
    WHEN is_account_group_member('governance_auditors') THEN input_address
    WHEN is_account_group_member('customer_support') THEN input_address
    ELSE 'MASKED_ADDRESS'
  END;

-- Explanation:
--   Authorized groups see real address.
--   Others see the literal replacement MASKED_ADDRESS.


-- ============================================================
-- STEP 5: APPLY COLUMN MASKS
-- ============================================================
-- This is the actual Unity Catalog syntax for attaching masks
-- to existing columns.
--
-- After this step, any SELECT on these columns will pass through
-- the masking functions automatically.
-- ============================================================

ALTER TABLE ecomsphere.silver.silver_dim_customer_profile_masking_demo
ALTER COLUMN email
SET MASK ecomsphere.governance.mask_email;

ALTER TABLE ecomsphere.silver.silver_dim_customer_profile_masking_demo
ALTER COLUMN phone_number
SET MASK ecomsphere.governance.mask_phone;

ALTER TABLE ecomsphere.silver.silver_dim_customer_profile_masking_demo
ALTER COLUMN address
SET MASK ecomsphere.governance.mask_address;


-- ============================================================
-- STEP 6: REGISTER METADATA
-- ============================================================
-- Optional but useful for governance reporting and documentation.
-- This assumes Exercise 1 dataset_metadata table already exists.
-- ============================================================

INSERT INTO ecomsphere.governance.dataset_metadata
VALUES
('table',  'ecomsphere.silver.silver_dim_customer_profile_masking_demo',         'customer', 'silver', 'data_engineers', 'on demand', 'pii', 'Practice table for Unity Catalog masking', current_timestamp()),
('column', 'ecomsphere.silver.silver_dim_customer_profile_masking_demo.email',   'customer', 'silver', 'data_engineers', 'on demand', 'pii', 'Masked practice email column', current_timestamp()),
('column', 'ecomsphere.silver.silver_dim_customer_profile_masking_demo.phone_number', 'customer', 'silver', 'data_engineers', 'on demand', 'pii', 'Masked practice phone column', current_timestamp()),
('column', 'ecomsphere.silver.silver_dim_customer_profile_masking_demo.address', 'customer', 'silver', 'data_engineers', 'on demand', 'pii', 'Masked practice address column', current_timestamp());


-- ============================================================
-- STEP 7: VALIDATION QUERY
-- ============================================================
-- Run this after masks are applied.
--
-- What you will see depends on the group membership of the user
-- running the query.
-- ============================================================

SELECT
    customer_id,
    customer_name,
    email,
    phone_number,
    address,
    city,
    country
FROM ecomsphere.silver.silver_dim_customer_profile_masking_demo
ORDER BY customer_id;


-- ============================================================
-- STEP 8: VERIFY THAT MASKS ARE ATTACHED
-- ============================================================
-- Databricks exposes metadata about attached column masks in
-- INFORMATION_SCHEMA.COLUMN_MASKS.
--
-- This is useful for governance validation and audits.
-- ============================================================

SELECT *
FROM ecomsphere.information_schema.column_masks
WHERE table_schema = 'silver'
  AND table_name = 'silver_dim_customer_profile_masking_demo'
ORDER BY column_name;


-- ============================================================
-- STEP 9: OPTIONAL CLEANUP / CHANGES
-- ============================================================
-- If you want to remove a mask later, use DROP MASK.
-- Uncomment only if you actually want to remove it.
-- ============================================================

-- ALTER TABLE ecomsphere.silver.silver_dim_customer_profile_masking_demo
-- ALTER COLUMN email
-- DROP MASK;

-- ALTER TABLE ecomsphere.silver.silver_dim_customer_profile_masking_demo
-- ALTER COLUMN phone_number
-- DROP MASK;

-- ALTER TABLE ecomsphere.silver.silver_dim_customer_profile_masking_demo
-- ALTER COLUMN address
-- DROP MASK;


-- ============================================================
-- STEP 10: TESTING NOTES
-- ============================================================
-- To properly test masking behavior:
--
-- 1. Run the SELECT as a user in governance_auditors
--    -> should see original values
--
-- 2. Run the SELECT as a user in customer_support
--    -> should see original values
--
-- 3. Run the SELECT as a user in data_analysts
--    -> should see masked values
--
-- If you are testing from only one login/session, you will only
-- observe the behavior for that current user's group membership.
-- ============================================================