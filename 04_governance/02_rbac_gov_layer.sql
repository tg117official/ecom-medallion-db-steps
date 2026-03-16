-- ============================================================
-- EXERCISE 2: ROLE-BASED ACCESS CONTROL (RBAC)
-- UPDATED FOR UNITY CATALOG
-- ============================================================
--
-- Why the earlier script failed:
-- The clause:
--   ON ALL TABLES IN SCHEMA ...
-- caused the parse error.
--
-- In Unity Catalog, privileges are inherited.
-- So instead of granting on "all tables in schema",
-- we grant SELECT / MODIFY directly on the SCHEMA.
--
-- This automatically covers current and future tables/views
-- as supported by the privilege type.
--
-- Assumed groups already exist:
--   data_engineers
--   data_analysts
--   finance_analysts
--   customer_support
--   governance_auditors
-- ============================================================



-- ============================================================
-- SECTION 1: CATALOG ACCESS
-- ============================================================
-- USE CATALOG:
-- Allows the group to access the ecomsphere catalog namespace.
-- Without this, the user cannot properly access child schemas.
-- ============================================================

GRANT USE CATALOG ON CATALOG ecomsphere TO `data_engineers`;
GRANT USE CATALOG ON CATALOG ecomsphere TO `data_analysts`;
GRANT USE CATALOG ON CATALOG ecomsphere TO `finance_analysts`;
GRANT USE CATALOG ON CATALOG ecomsphere TO `customer_support`;
GRANT USE CATALOG ON CATALOG ecomsphere TO `governance_auditors`;


-- ============================================================
-- SECTION 2: DATA ENGINEERS
-- ============================================================
-- Goal:
-- Engineers need broad working access across Bronze, Silver, Gold,
-- and read access to Governance.
--
-- USE SCHEMA:
-- Allows access to the schema namespace.
--
-- SELECT ON SCHEMA:
-- Gives read/query access to all current and future tables/views
-- in that schema through privilege inheritance.
--
-- MODIFY ON SCHEMA:
-- Gives ability to insert/update/delete/merge data in all current
-- and future tables in that schema, provided other required access
-- is also available.
-- ============================================================

GRANT USE SCHEMA ON SCHEMA ecomsphere.bronze TO `data_engineers`;
-- data_engineers can access the bronze schema namespace.

GRANT USE SCHEMA ON SCHEMA ecomsphere.silver TO `data_engineers`;
-- data_engineers can access the silver schema namespace.

GRANT USE SCHEMA ON SCHEMA ecomsphere.gold TO `data_engineers`;
-- data_engineers can access the gold schema namespace.

GRANT USE SCHEMA ON SCHEMA ecomsphere.governance TO `data_engineers`;
-- data_engineers can access the governance schema namespace.

GRANT SELECT ON SCHEMA ecomsphere.bronze TO `data_engineers`;
-- data_engineers can read/query all current and future tables/views
-- in the bronze schema.

GRANT MODIFY ON SCHEMA ecomsphere.bronze TO `data_engineers`;
-- data_engineers can modify data in all current and future bronze
-- tables (insert/update/delete/merge).

GRANT SELECT ON SCHEMA ecomsphere.silver TO `data_engineers`;
-- data_engineers can read/query all current and future silver tables/views.

GRANT MODIFY ON SCHEMA ecomsphere.silver TO `data_engineers`;
-- data_engineers can modify data in all current and future silver tables.

GRANT SELECT ON SCHEMA ecomsphere.gold TO `data_engineers`;
-- data_engineers can read/query all current and future gold tables/views.

GRANT MODIFY ON SCHEMA ecomsphere.gold TO `data_engineers`;
-- data_engineers can modify data in all current and future gold tables.

GRANT SELECT ON SCHEMA ecomsphere.governance TO `data_engineers`;
-- data_engineers can read/query all current and future governance
-- tables/views.
--
-- We are intentionally NOT granting MODIFY on governance here,
-- because governance tables are usually more tightly controlled.


-- ============================================================
-- SECTION 3: DATA ANALYSTS
-- ============================================================
-- Goal:
-- Analysts should consume curated business-ready data from Gold.
-- They should not access Bronze or Silver in this design.
-- ============================================================

GRANT USE SCHEMA ON SCHEMA ecomsphere.gold TO `data_analysts`;
-- data_analysts can access the gold schema namespace.

GRANT SELECT ON SCHEMA ecomsphere.gold TO `data_analysts`;
-- data_analysts can read/query all current and future gold tables/views.


-- ============================================================
-- SECTION 4: FINANCE ANALYSTS
-- ============================================================
-- Goal:
-- Finance analysts should not get all Gold access.
-- They should get selective table-level access only.
--
-- So:
--   1. give USE SCHEMA on gold
--   2. grant SELECT only on required tables
-- ============================================================

GRANT USE SCHEMA ON SCHEMA ecomsphere.gold TO `finance_analysts`;
-- finance_analysts can access the gold schema namespace.

GRANT SELECT ON TABLE ecomsphere.gold.fact_sales_order_item TO `finance_analysts`;
-- finance_analysts can read the fact_sales_order_item table.

GRANT SELECT ON TABLE ecomsphere.gold.gold_daily_sales_summary TO `finance_analysts`;
-- finance_analysts can read the daily sales summary table.

GRANT SELECT ON TABLE ecomsphere.gold.gold_customer_360_summary TO `finance_analysts`;
-- finance_analysts can read customer summary data if required
-- for financial/customer value analysis.


-- ============================================================
-- SECTION 5: CUSTOMER SUPPORT
-- ============================================================
-- Goal:
-- Customer support gets only the operational serving datasets
-- required for support workflows.
-- ============================================================

GRANT USE SCHEMA ON SCHEMA ecomsphere.gold TO `customer_support`;
-- customer_support can access the gold schema namespace.

GRANT SELECT ON TABLE ecomsphere.gold.customer_360_document TO `customer_support`;
-- customer_support can read the customer_360 serving model.

GRANT SELECT ON TABLE ecomsphere.gold.order_tracking_document TO `customer_support`;
-- customer_support can read the order_tracking serving model.


-- ============================================================
-- SECTION 6: GOVERNANCE AUDITORS
-- ============================================================
-- Goal:
-- Governance auditors need full read access to governance objects
-- and selected business tables for validation/audit.
-- ============================================================

GRANT USE SCHEMA ON SCHEMA ecomsphere.governance TO `governance_auditors`;
-- governance_auditors can access the governance schema namespace.

GRANT SELECT ON SCHEMA ecomsphere.governance TO `governance_auditors`;
-- governance_auditors can read all current and future tables/views
-- in the governance schema.

GRANT USE SCHEMA ON SCHEMA ecomsphere.gold TO `governance_auditors`;
-- governance_auditors can access the gold schema namespace.

GRANT SELECT ON TABLE ecomsphere.gold.fact_sales_order_item TO `governance_auditors`;
-- governance_auditors can read the sales fact table.

GRANT SELECT ON TABLE ecomsphere.gold.dim_customer TO `governance_auditors`;
-- governance_auditors can read the customer dimension.


-- ============================================================
-- SECTION 7: VALIDATION
-- ============================================================
-- SHOW GRANTS is used to verify assigned permissions.
-- ============================================================

SHOW GRANTS ON SCHEMA ecomsphere.gold;
-- Displays grants on the gold schema.

SHOW GRANTS ON TABLE ecomsphere.gold.fact_sales_order_item;
-- Displays grants on the specific table.