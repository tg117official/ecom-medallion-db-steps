%sql
-- ============================================================
-- EXERCISE 6: METADATA AND DATA DISCOVERY
-- DIRECT PRACTICE VERSION
-- ============================================================
--
-- OBJECTIVE:
-- Learn how to improve discoverability by documenting tables
-- and columns using comments and by registering business
-- metadata in a governance table.
--
-- ------------------------------------------------------------
-- BEGINNER NOTE: WHAT IS METADATA?
-- ------------------------------------------------------------
-- Metadata means "data about data".
--
-- Examples:
--   - what this table is used for
--   - what a column means
--   - who owns the dataset
--   - how often it refreshes
--   - whether it contains PII or confidential data
--
-- Why this matters:
-- Governance is not only about restricting access.
-- Governance also means making trusted data easy to:
--   1. find
--   2. understand
--   3. use correctly
--
-- In this exercise, we will:
--   1. create demo Silver and Gold tables
--   2. add table comments
--   3. add column comments
--   4. register business metadata in governance.dataset_metadata
--   5. validate using DESCRIBE and INFORMATION_SCHEMA
--
-- ============================================================


-- ============================================================
-- STEP 0: CLEANUP
-- ============================================================
-- This makes the exercise rerunnable.
-- ============================================================

DROP TABLE IF EXISTS ecomsphere.silver.silver_dim_customer_profile_meta_demo;
DROP TABLE IF EXISTS ecomsphere.silver.silver_fact_order_line_enriched_meta_demo;
DROP TABLE IF EXISTS ecomsphere.gold.dim_customer_meta_demo;
DROP TABLE IF EXISTS ecomsphere.gold.fact_sales_order_item_meta_demo;


-- ============================================================
-- STEP 1: CREATE DEMO TABLES
-- ============================================================
-- We are creating a few small demo tables so that metadata
-- documentation can be practiced safely and directly.
-- ============================================================

CREATE TABLE IF NOT EXISTS ecomsphere.silver.silver_dim_customer_profile_meta_demo (
    customer_id        STRING,
    customer_name      STRING,
    email              STRING,
    phone_number       STRING,
    city               STRING,
    country            STRING,
    customer_status    STRING,
    created_ts         TIMESTAMP
)
USING DELTA;

CREATE TABLE IF NOT EXISTS ecomsphere.silver.silver_fact_order_line_enriched_meta_demo (
    order_id           STRING,
    customer_id        STRING,
    product_id         STRING,
    order_status       STRING,
    quantity           INT,
    unit_price         DECIMAL(18,2),
    net_amount         DECIMAL(18,2),
    order_ts           TIMESTAMP
)
USING DELTA;

CREATE TABLE IF NOT EXISTS ecomsphere.gold.dim_customer_meta_demo (
    customer_key       BIGINT,
    customer_id        STRING,
    customer_name      STRING,
    city               STRING,
    country            STRING,
    is_current         STRING,
    effective_from_ts  TIMESTAMP,
    effective_to_ts    TIMESTAMP
)
USING DELTA;

CREATE TABLE IF NOT EXISTS ecomsphere.gold.fact_sales_order_item_meta_demo (
    sales_key          BIGINT,
    order_id           STRING,
    customer_key       BIGINT,
    product_key        BIGINT,
    order_status       STRING,
    quantity           INT,
    net_amount         DECIMAL(18,2),
    order_ts           TIMESTAMP
)
USING DELTA;


-- ============================================================
-- STEP 2: INSERT SAMPLE DATA
-- ============================================================
-- Metadata works even without data, but sample rows make the
-- tables feel more realistic when you explore them.
-- ============================================================

INSERT INTO ecomsphere.silver.silver_dim_customer_profile_meta_demo
VALUES
('C001', 'Aarav Sharma', 'aarav@example.com', '9876543210', 'Pune',    'India', 'ACTIVE',   current_timestamp()),
('C002', 'Meera Iyer',   'meera@example.com', '9123456780', 'Chennai', 'India', 'ACTIVE',   current_timestamp()),
('C003', 'Rohan Verma',  'rohan@example.com', '9988776655', 'Mumbai',  'India', 'INACTIVE', current_timestamp());

INSERT INTO ecomsphere.silver.silver_fact_order_line_enriched_meta_demo
VALUES
('O1001', 'C001', 'P001', 'DELIVERED', 2, 1200.00, 2400.00, current_timestamp()),
('O1002', 'C002', 'P002', 'SHIPPED',   1,  850.00,  850.00, current_timestamp()),
('O1003', 'C003', 'P003', 'PENDING',   3,  650.00, 1950.00, current_timestamp());

INSERT INTO ecomsphere.gold.dim_customer_meta_demo
VALUES
(101, 'C001', 'Aarav Sharma', 'Pune',    'India', 'Y', current_timestamp(), NULL),
(102, 'C002', 'Meera Iyer',   'Chennai', 'India', 'Y', current_timestamp(), NULL),
(103, 'C003', 'Rohan Verma',  'Mumbai',  'India', 'Y', current_timestamp(), NULL);

INSERT INTO ecomsphere.gold.fact_sales_order_item_meta_demo
VALUES
(10001, 'O1001', 101, 501, 'DELIVERED', 2, 2400.00, current_timestamp()),
(10002, 'O1002', 102, 502, 'SHIPPED',   1,  850.00, current_timestamp()),
(10003, 'O1003', 103, 503, 'PENDING',   3, 1950.00, current_timestamp());


-- ============================================================
-- STEP 3: ADD TABLE COMMENTS
-- ============================================================
-- Table comments explain the purpose of the whole dataset.
-- These comments help users discover tables in Catalog Explorer
-- and understand what each table is meant for.
-- ============================================================

COMMENT ON TABLE ecomsphere.silver.silver_dim_customer_profile_meta_demo IS
'Curated customer profile table used for customer analytics and downstream dimensional modeling';

COMMENT ON TABLE ecomsphere.silver.silver_fact_order_line_enriched_meta_demo IS
'Validated and enriched order-line dataset in Silver layer before Gold fact loading';

COMMENT ON TABLE ecomsphere.gold.dim_customer_meta_demo IS
'SCD-style customer dimension used for historical customer analysis in the Gold layer';

COMMENT ON TABLE ecomsphere.gold.fact_sales_order_item_meta_demo IS
'Order-item grain sales fact table used for finance reporting and operational analytics';


-- ============================================================
-- STEP 4: ADD COLUMN COMMENTS
-- ============================================================
-- Column comments explain what an individual field means.
-- This is extremely useful when users are unsure about:
--   - surrogate keys
--   - business identifiers
--   - status columns
--   - measures like amount
-- ============================================================

COMMENT ON COLUMN ecomsphere.gold.fact_sales_order_item_meta_demo.order_id IS
'Business order identifier coming from the source transactional system';

COMMENT ON COLUMN ecomsphere.gold.fact_sales_order_item_meta_demo.customer_key IS
'Surrogate key referencing the Gold customer dimension';

COMMENT ON COLUMN ecomsphere.gold.fact_sales_order_item_meta_demo.product_key IS
'Surrogate key referencing the Gold product dimension';

COMMENT ON COLUMN ecomsphere.gold.fact_sales_order_item_meta_demo.net_amount IS
'Final order-item amount after validations, enrichment, and business rule application';

COMMENT ON COLUMN ecomsphere.gold.dim_customer_meta_demo.customer_key IS
'Surrogate key generated for the customer dimension';

COMMENT ON COLUMN ecomsphere.gold.dim_customer_meta_demo.is_current IS
'Flag indicating whether the row represents the current active version of the customer record';

COMMENT ON COLUMN ecomsphere.silver.silver_dim_customer_profile_meta_demo.email IS
'Customer email address captured from source systems';

COMMENT ON COLUMN ecomsphere.silver.silver_fact_order_line_enriched_meta_demo.net_amount IS
'Calculated order-line amount in curated Silver layer before Gold loading';


-- ============================================================
-- STEP 5: OPTIONAL - ADD SCHEMA COMMENTS
-- ============================================================
-- Schema comments help explain the purpose of the entire layer.
-- ============================================================

COMMENT ON SCHEMA ecomsphere.silver IS
'Curated conformed layer containing cleansed, validated, and integrated datasets';

COMMENT ON SCHEMA ecomsphere.gold IS
'Analytics and serving layer containing dimensions, facts, summaries, and serving models';


-- ============================================================
-- STEP 6: REGISTER BUSINESS METADATA
-- ============================================================
-- This step populates your governance table with metadata
-- records that are useful for reporting and discovery.
--
-- We store business-oriented metadata such as:
--   - domain
--   - layer
--   - owner
--   - sensitivity
--   - refresh frequency
--   - description
-- ============================================================

INSERT INTO ecomsphere.governance.dataset_metadata
VALUES
('table', 'ecomsphere.silver.silver_dim_customer_profile_meta_demo',     'customer', 'silver', 'data_engineers',   '6 hours', 'pii',          'Curated customer profile table for analytics and downstream dimensional use', current_timestamp()),
('table', 'ecomsphere.silver.silver_fact_order_line_enriched_meta_demo', 'sales',    'silver', 'data_engineers',   '6 hours', 'confidential', 'Validated and enriched order-line table before Gold fact loading', current_timestamp()),
('table', 'ecomsphere.gold.dim_customer_meta_demo',                      'customer', 'gold',   'data_analysts',    '6 hours', 'pii',          'Customer dimension with historical tracking behavior', current_timestamp()),
('table', 'ecomsphere.gold.fact_sales_order_item_meta_demo',             'sales',    'gold',   'finance_analysts', '6 hours', 'confidential', 'Order-item sales fact table for finance and reporting use cases', current_timestamp()),

('column', 'ecomsphere.gold.fact_sales_order_item_meta_demo.order_id',      'sales',    'gold',   'finance_analysts', '6 hours', 'internal',      'Business order identifier in Gold fact table', current_timestamp()),
('column', 'ecomsphere.gold.fact_sales_order_item_meta_demo.customer_key',  'sales',    'gold',   'data_engineers',   '6 hours', 'internal',      'Surrogate foreign key to customer dimension', current_timestamp()),
('column', 'ecomsphere.gold.fact_sales_order_item_meta_demo.product_key',   'sales',    'gold',   'data_engineers',   '6 hours', 'internal',      'Surrogate foreign key to product dimension', current_timestamp()),
('column', 'ecomsphere.gold.fact_sales_order_item_meta_demo.net_amount',    'sales',    'gold',   'finance_analysts', '6 hours', 'confidential',  'Final order-item amount used in reporting and analytics', current_timestamp()),
('column', 'ecomsphere.gold.dim_customer_meta_demo.customer_key',           'customer', 'gold',   'data_engineers',   '6 hours', 'internal',      'Surrogate key generated for the customer dimension', current_timestamp()),
('column', 'ecomsphere.gold.dim_customer_meta_demo.is_current',             'customer', 'gold',   'data_engineers',   '6 hours', 'internal',      'Current row indicator for dimensional history handling', current_timestamp()),
('column', 'ecomsphere.silver.silver_dim_customer_profile_meta_demo.email', 'customer', 'silver', 'data_engineers',   '6 hours', 'pii',           'Customer email column in Silver customer profile', current_timestamp()),
('column', 'ecomsphere.silver.silver_fact_order_line_enriched_meta_demo.net_amount', 'sales', 'silver', 'data_engineers', '6 hours', 'confidential', 'Calculated Silver order-line amount before Gold loading', current_timestamp());


-- ============================================================
-- STEP 7: VALIDATION USING YOUR GOVERNANCE TABLE
-- ============================================================
-- This shows your custom metadata registry.
-- ============================================================

SELECT
    full_name,
    object_type,
    business_domain,
    data_layer,
    owner_group,
    sensitivity_class,
    description
FROM ecomsphere.governance.dataset_metadata
WHERE full_name LIKE 'ecomsphere.%meta_demo%'
ORDER BY object_type, full_name;


-- ============================================================
-- STEP 8: VALIDATION USING DESCRIBE TABLE
-- ============================================================
-- DESCRIBE TABLE shows column metadata, including comments.
-- This is one of the easiest ways to confirm column comments.
-- ============================================================

DESCRIBE TABLE ecomsphere.gold.fact_sales_order_item_meta_demo;


-- ============================================================
-- STEP 9: VALIDATION USING INFORMATION_SCHEMA.TABLES
-- ============================================================
-- INFORMATION_SCHEMA is Databricks' metadata interface.
-- It is privilege-aware, meaning users only see objects they
-- are allowed to access.
-- ============================================================

SELECT
    table_catalog,
    table_schema,
    table_name,
    comment
FROM ecomsphere.information_schema.tables
WHERE table_schema IN ('silver', 'gold')
  AND table_name IN (
      'silver_dim_customer_profile_meta_demo',
      'silver_fact_order_line_enriched_meta_demo',
      'dim_customer_meta_demo',
      'fact_sales_order_item_meta_demo'
  )
ORDER BY table_schema, table_name;


-- ============================================================
-- STEP 10: VALIDATION USING INFORMATION_SCHEMA.COLUMNS
-- ============================================================
-- This view helps validate column-level metadata.
-- ============================================================

SELECT
    table_catalog,
    table_schema,
    table_name,
    column_name,
    full_data_type,
    comment
FROM ecomsphere.information_schema.columns
WHERE table_schema IN ('silver', 'gold')
  AND table_name IN (
      'silver_dim_customer_profile_meta_demo',
      'silver_fact_order_line_enriched_meta_demo',
      'dim_customer_meta_demo',
      'fact_sales_order_item_meta_demo'
  )
ORDER BY table_schema, table_name, ordinal_position;


-- ============================================================
-- STEP 11: WHAT YOU SHOULD OBSERVE
-- ============================================================
--
-- 1. Table comments explain dataset purpose.
-- 2. Column comments explain field meaning.
-- 3. Governance table stores business-facing metadata.
-- 4. INFORMATION_SCHEMA lets you query metadata using SQL.
-- 5. This improves discovery, trust, and usability.
--
-- ============================================================