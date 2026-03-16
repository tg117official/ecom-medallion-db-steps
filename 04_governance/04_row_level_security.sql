-- ============================================================
-- EXERCISE 4: ROW-LEVEL SECURITY (RLS) BY REGION
-- DIRECT PRACTICE VERSION
-- ============================================================
--
-- Objective:
-- Practice Unity Catalog row-level security end-to-end using
-- a small sample sales fact table.
--
-- What this script does:
--   1. Creates a practice Gold table
--   2. Inserts sample rows across multiple regions
--   3. Creates a row filter function in governance schema
--   4. Applies the row filter to the table
--   5. Validates behavior
--
-- Important prerequisites:
--   - Table must be in Unity Catalog
--   - Row filters are Unity Catalog only
--   - ALTER TABLE ... SET ROW FILTER is supported in
--     Databricks SQL / DBR 12.2 LTS+
--
-- Assumed groups already exist:
--   governance_auditors
--   finance_analysts
--   customer_support_west
--   customer_support_north
--   customer_support_south
--
-- Access logic in this exercise:
--   governance_auditors -> can see all rows
--   finance_analysts    -> can see all rows
--   customer_support_west  -> only WEST rows
--   customer_support_north -> only NORTH rows
--   customer_support_south -> only SOUTH rows
--   everyone else          -> no rows
--
-- IMPORTANT NOTE ABOUT ADMINS:
--   Even if a user is admin, the query result still depends on:
--     1. SELECT privilege on the table
--     2. the row filter function logic
--   If the function does not explicitly allow that user/group,
--   rows may still be filtered out.
--
-- ============================================================


-- ============================================================
-- STEP 0: OPTIONAL CLEANUP
-- ============================================================
-- This makes the script easy to rerun for practice.
-- ============================================================

DROP TABLE IF EXISTS ecomsphere.gold.fact_sales_order_item_rls_demo;

-- Optional:
-- if you want to remove old function logic entirely, CREATE OR REPLACE
-- below is enough, so explicit DROP FUNCTION is not required.


-- ============================================================
-- STEP 1: CREATE PRACTICE TABLE
-- ============================================================
-- We create a small Gold-layer demo table instead of depending on
-- your real fact_sales_order_item table.
-- ============================================================

CREATE TABLE IF NOT EXISTS ecomsphere.gold.fact_sales_order_item_rls_demo (
    sales_id         STRING,
    order_id         STRING,
    customer_id      STRING,
    product_id       STRING,
    region           STRING,
    sales_channel    STRING,
    order_status     STRING,
    quantity         INT,
    net_amount       DECIMAL(18,2),
    order_ts         TIMESTAMP
)
USING DELTA;

COMMENT ON TABLE ecomsphere.gold.fact_sales_order_item_rls_demo IS
'Practice table for learning Unity Catalog row-level security by region';

COMMENT ON COLUMN ecomsphere.gold.fact_sales_order_item_rls_demo.region IS
'Region used for row-level security filtering';


-- ============================================================
-- STEP 2: INSERT SAMPLE DATA
-- ============================================================
-- We insert rows for multiple regions so that row filtering is
-- easy to observe in query results.
-- ============================================================

INSERT INTO ecomsphere.gold.fact_sales_order_item_rls_demo
VALUES
('S001', 'O1001', 'C001', 'P001', 'WEST',  'APP',    'SHIPPED',   2, 1200.00, current_timestamp()),
('S002', 'O1002', 'C002', 'P002', 'WEST',  'WEB',    'DELIVERED', 1,  850.00, current_timestamp()),
('S003', 'O1003', 'C003', 'P003', 'NORTH', 'APP',    'PENDING',   3, 2100.00, current_timestamp()),
('S004', 'O1004', 'C004', 'P004', 'NORTH', 'STORE',  'SHIPPED',   1,  650.00, current_timestamp()),
('S005', 'O1005', 'C005', 'P005', 'SOUTH', 'WEB',    'DELIVERED', 4, 3400.00, current_timestamp()),
('S006', 'O1006', 'C006', 'P006', 'SOUTH', 'APP',    'CANCELLED', 1,  500.00, current_timestamp()),
('S007', 'O1007', 'C007', 'P007', 'EAST',  'WEB',    'DELIVERED', 2, 1800.00, current_timestamp()),
('S008', 'O1008', 'C008', 'P008', 'EAST',  'STORE',  'PENDING',   5, 4200.00, current_timestamp());


-- ============================================================
-- STEP 3: PRE-FILTER VALIDATION
-- ============================================================
-- Before applying row security, users with SELECT privilege
-- will see all rows.
-- ============================================================

SELECT *
FROM ecomsphere.gold.fact_sales_order_item_rls_demo
ORDER BY sales_id;

SELECT region, COUNT(*) AS row_count, SUM(net_amount) AS total_amount
FROM ecomsphere.gold.fact_sales_order_item_rls_demo
GROUP BY region
ORDER BY region;


-- ============================================================
-- STEP 4: CREATE ROW FILTER FUNCTION
-- ============================================================
-- A row filter is a SQL UDF that returns BOOLEAN.
--
-- If the function returns:
--   TRUE  -> row is visible
--   FALSE -> row is hidden
--   NULL  -> row is hidden
--
-- The first parameter maps to the table column we use for filtering.
-- ============================================================

CREATE OR REPLACE FUNCTION ecomsphere.governance.region_row_filter(input_region STRING)
RETURNS BOOLEAN
COMMENT 'Restricts row visibility by region based on group membership'
RETURN
  CASE
    WHEN is_account_group_member('governance_auditors') THEN TRUE
    WHEN is_account_group_member('finance_analysts') THEN TRUE
    WHEN is_account_group_member('customer_support_west')  AND input_region = 'WEST'  THEN TRUE
    WHEN is_account_group_member('customer_support_north') AND input_region = 'NORTH' THEN TRUE
    WHEN is_account_group_member('customer_support_south') AND input_region = 'SOUTH' THEN TRUE
    ELSE FALSE
  END;

-- Explanation:
--   governance_auditors -> all rows visible
--   finance_analysts    -> all rows visible
--   customer_support_west  -> only rows where region = 'WEST'
--   customer_support_north -> only rows where region = 'NORTH'
--   customer_support_south -> only rows where region = 'SOUTH'
--   others -> zero rows visible


-- ============================================================
-- STEP 5: APPLY THE ROW FILTER
-- ============================================================
-- This is the actual Databricks Unity Catalog syntax.
--
-- ON (region) means:
--   Pass the region column value from each row into the function.
-- ============================================================

ALTER TABLE ecomsphere.gold.fact_sales_order_item_rls_demo
SET ROW FILTER ecomsphere.governance.region_row_filter ON (region);


-- ============================================================
-- STEP 6: REGISTER GOVERNANCE METADATA
-- ============================================================
-- Optional but useful if you are maintaining your governance
-- dataset metadata repository.
--
-- This assumes Exercise 1 created:
--   ecomsphere.governance.dataset_metadata
-- ============================================================

INSERT INTO ecomsphere.governance.dataset_metadata
VALUES
(
  'table',
  'ecomsphere.gold.fact_sales_order_item_rls_demo',
  'sales',
  'gold',
  'data_engineers',
  'on demand',
  'confidential',
  'Practice table for Unity Catalog row-level security by region',
  current_timestamp()
);


-- ============================================================
-- STEP 7: VALIDATION QUERIES
-- ============================================================
-- After the row filter is applied, the result now depends on
-- the group membership of the querying user.
--
-- Run this query as different users/groups to observe behavior.
-- ============================================================

SELECT *
FROM ecomsphere.gold.fact_sales_order_item_rls_demo
ORDER BY sales_id;

SELECT region, COUNT(*) AS visible_rows, SUM(net_amount) AS visible_amount
FROM ecomsphere.gold.fact_sales_order_item_rls_demo
GROUP BY region
ORDER BY region;

-- Expected examples:
--   user in governance_auditors -> sees WEST, NORTH, SOUTH, EAST
--   user in finance_analysts    -> sees WEST, NORTH, SOUTH, EAST
--   user in customer_support_west  -> sees only WEST rows
--   user in customer_support_north -> sees only NORTH rows
--   user in customer_support_south -> sees only SOUTH rows
--   user in no allowed group       -> sees zero rows


-- ============================================================
-- STEP 8: VERIFY THAT THE ROW FILTER IS ATTACHED
-- ============================================================
-- INFORMATION_SCHEMA.ROW_FILTERS contains row filter metadata.
-- This is useful for governance validation and audits.
-- ============================================================

SELECT *
FROM ecomsphere.information_schema.row_filters
WHERE table_schema = 'gold'
  AND table_name = 'fact_sales_order_item_rls_demo';


-- ============================================================
-- STEP 9: OPTIONAL TEST QUERIES TO UNDERSTAND BEHAVIOR
-- ============================================================
-- These help you understand that filtering is happening
-- automatically at read time.
-- ============================================================

-- Example 1: aggregate still respects row-level filter
SELECT COUNT(*) AS visible_count
FROM ecomsphere.gold.fact_sales_order_item_rls_demo;

-- Example 2: sums also respect row-level filter
SELECT SUM(net_amount) AS visible_sales
FROM ecomsphere.gold.fact_sales_order_item_rls_demo;

-- Example 3: distinct regions are limited by policy
SELECT DISTINCT region
FROM ecomsphere.gold.fact_sales_order_item_rls_demo
ORDER BY region;


-- ============================================================
-- STEP 10: OPTIONAL REMOVE THE ROW FILTER
-- ============================================================
-- Uncomment this only if you want to remove the policy later.
-- ============================================================

-- ALTER TABLE ecomsphere.gold.fact_sales_order_item_rls_demo
-- DROP ROW FILTER;


-- ============================================================
-- STEP 11: TESTING NOTES
-- ============================================================
-- To properly test row-level security:
--
-- 1. Run the SELECT as a user in governance_auditors
--    -> should see all rows
--
-- 2. Run the SELECT as a user in finance_analysts
--    -> should see all rows
--
-- 3. Run the SELECT as a user in customer_support_west
--    -> should see only WEST rows
--
-- 4. Run the SELECT as a user in customer_support_north
--    -> should see only NORTH rows
--
-- 5. Run the SELECT as a user in customer_support_south
--    -> should see only SOUTH rows
--
-- 6. Run the SELECT as a user not in any allowed group
--    -> should see zero rows
--
-- If you test from only one login/session, you will observe
-- only the behavior for that user's group membership.
-- ============================================================