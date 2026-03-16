-- ============================================================
-- EXERCISE 5: TAGS / GOVERNED TAGS
-- DIRECT PRACTICE VERSION FOR BEGINNERS
-- ============================================================
--
-- OBJECTIVE:
-- Learn what tags are, why they are used, and how to apply them
-- to Unity Catalog objects using SQL.
--
-- ------------------------------------------------------------
-- BEGINNER NOTE: WHAT IS A TAG?
-- ------------------------------------------------------------
-- A tag is like a label attached to a data object.
--
-- Example labels:
--   data_classification = pii
--   business_domain     = customer
--   quality_status      = certified
--
-- Why do we use tags?
--   1. To classify data
--   2. To make data easier to discover
--   3. To prepare for policy-based governance later
--   4. To make metadata more meaningful
--
-- Simple analogy:
--   Think of tags like labels on folders:
--     "Confidential"
--     "Finance"
--     "Customer"
--     "Certified"
--
-- In Databricks Unity Catalog:
--   tags can be attached to:
--     catalog / schema / table / view / volume / column
--
-- Governed tags are a stricter form of tags:
--   - centrally controlled
--   - allowed values are predefined
--   - only authorized users can assign them
--
-- In this exercise we will focus on:
--   1. creating a demo table
--   2. applying tags using SQL
--   3. understanding how tags help governance
--
-- ============================================================


-- ============================================================
-- STEP 0: CLEANUP
-- ============================================================
-- This makes the exercise rerunnable.
-- ============================================================

DROP TABLE IF EXISTS ecomsphere.gold.product_360_tag_demo;


-- ============================================================
-- STEP 1: CREATE A PRACTICE TABLE
-- ============================================================
-- We create a small Gold-layer demo table.
-- Later we will attach tags to:
--   - the table itself
--   - some individual columns
-- ============================================================

CREATE TABLE IF NOT EXISTS ecomsphere.gold.product_360_tag_demo (
    product_id          STRING,
    product_name        STRING,
    category_name       STRING,
    brand_name          STRING,
    supplier_cost       DECIMAL(18,2),
    selling_price       DECIMAL(18,2),
    product_status      STRING,
    review_score        DECIMAL(5,2),
    created_ts          TIMESTAMP
)
USING DELTA;

COMMENT ON TABLE ecomsphere.gold.product_360_tag_demo IS
'Practice table for learning Unity Catalog tags and governed tags';

COMMENT ON COLUMN ecomsphere.gold.product_360_tag_demo.supplier_cost IS
'Sensitive commercial cost metric that may need controlled access';

COMMENT ON COLUMN ecomsphere.gold.product_360_tag_demo.selling_price IS
'Public or internal selling price depending on policy';

COMMENT ON COLUMN ecomsphere.gold.product_360_tag_demo.review_score IS
'Customer-facing product quality score';


-- ============================================================
-- STEP 2: INSERT SAMPLE DATA
-- ============================================================
-- This is not strictly required for tags, because tags are
-- metadata and not row data.
-- But inserting rows makes the table feel more realistic.
-- ============================================================

INSERT INTO ecomsphere.gold.product_360_tag_demo
VALUES
('P001', 'Wireless Headphones', 'Electronics', 'SoundMax', 1800.00, 2499.00, 'ACTIVE', 4.5, current_timestamp()),
('P002', 'Yoga Mat',            'Fitness',     'FlexFit',   350.00,  799.00, 'ACTIVE', 4.2, current_timestamp()),
('P003', 'Coffee Maker',        'Kitchen',     'HomeBrew', 2200.00, 3299.00, 'ACTIVE', 4.0, current_timestamp()),
('P004', 'Office Chair',        'Furniture',   'ErgoSit',  4200.00, 5999.00, 'INACTIVE', 3.8, current_timestamp());


-- ============================================================
-- STEP 3: UNDERSTAND WHAT WE ARE GOING TO TAG
-- ============================================================
-- We will attach tags to:
--
-- TABLE-LEVEL TAGS:
--   data_classification = internal
--   business_domain     = product
--
-- COLUMN-LEVEL TAGS:
--   supplier_cost       -> data_classification = confidential
--   review_score        -> quality_status      = certified
--
-- Why?
--   product_360 table belongs to the product domain
--   supplier_cost is commercially sensitive
--   review_score is a business metric we may mark as trusted
-- ============================================================


-- ============================================================
-- STEP 4: APPLY TAGS TO THE TABLE
-- ============================================================
-- IMPORTANT:
-- The syntax below uses SET TAG, which attaches metadata labels
-- to Unity Catalog objects.
--
-- A tag has:
--   key
--   value
--
-- Example:
--   key   = business_domain
--   value = product
--
-- Table-level tags classify the whole table.
-- ============================================================

SET TAG ON TABLE ecomsphere.gold.product_360_tag_demo
`business_domain` = `product`;

-- Meaning:
--   This table belongs to the product business domain.

SET TAG ON TABLE ecomsphere.gold.product_360_tag_demo
`data_classification` = `internal`;

-- Meaning:
--   This table should be treated as internal business data.

SET TAG ON TABLE ecomsphere.gold.product_360_tag_demo
`lifecycle_status` = `active`;

-- Meaning:
--   This table is actively used and not deprecated.


-- ============================================================
-- STEP 5: APPLY TAGS TO INDIVIDUAL COLUMNS
-- ============================================================
-- Column-level tags are useful when only specific fields are
-- sensitive or important.
--
-- This is very common in governance:
--   one table may be broadly internal,
--   but one column inside it may be confidential or PII.
-- ============================================================

SET TAG ON COLUMN ecomsphere.gold.product_360_tag_demo.supplier_cost
`data_classification` = `confidential`;

-- Meaning:
--   supplier_cost is more sensitive than normal product fields.
--   Not every user should necessarily see this column.

SET TAG ON COLUMN ecomsphere.gold.product_360_tag_demo.supplier_cost
`business_owner` = `procurement_team`;

-- Meaning:
--   Procurement team is the business owner of this cost data.

SET TAG ON COLUMN ecomsphere.gold.product_360_tag_demo.review_score
`quality_status` = `certified`;

-- Meaning:
--   This metric is considered trusted / validated for use.

SET TAG ON COLUMN ecomsphere.gold.product_360_tag_demo.selling_price
`data_classification` = `internal`;

-- Meaning:
--   selling_price is marked internal in this example.


-- ============================================================
-- STEP 6: OPTIONAL - APPLY A TAG TO THE SCHEMA
-- ============================================================
-- Tags can also be applied higher up, such as on a schema.
--
-- This helps classify an entire collection of objects.
--
-- Important concept:
-- Governed tags applied to catalog or schema can be inherited by
-- contained objects, except individual table columns.
-- ============================================================

SET TAG ON SCHEMA ecomsphere.gold
`layer_type` = `analytics`;

-- Meaning:
--   the gold schema represents the analytics / serving layer.


-- ============================================================
-- STEP 7: REGISTER TAG MEANING IN YOUR GOVERNANCE TABLE
-- ============================================================
-- This step is optional but useful for learning and reporting.
-- It assumes your earlier governance table exists:
--   ecomsphere.governance.asset_tag_registry
--
-- If it does not exist, we create it first.
-- ============================================================

CREATE TABLE IF NOT EXISTS ecomsphere.governance.asset_tag_registry (
    full_name      STRING,
    column_name    STRING,
    tag_key        STRING,
    tag_value      STRING,
    created_ts     TIMESTAMP
)
USING DELTA;

COMMENT ON TABLE ecomsphere.governance.asset_tag_registry IS
'Tracks tag assignments for governance reporting and learning exercises';

INSERT INTO ecomsphere.governance.asset_tag_registry VALUES
('ecomsphere.gold.product_360_tag_demo', NULL, 'business_domain', 'product', current_timestamp()),
('ecomsphere.gold.product_360_tag_demo', NULL, 'data_classification', 'internal', current_timestamp()),
('ecomsphere.gold.product_360_tag_demo', NULL, 'lifecycle_status', 'active', current_timestamp()),
('ecomsphere.gold.product_360_tag_demo', 'supplier_cost', 'data_classification', 'confidential', current_timestamp()),
('ecomsphere.gold.product_360_tag_demo', 'supplier_cost', 'business_owner', 'procurement_team', current_timestamp()),
('ecomsphere.gold.product_360_tag_demo', 'review_score', 'quality_status', 'certified', current_timestamp()),
('ecomsphere.gold.product_360_tag_demo', 'selling_price', 'data_classification', 'internal', current_timestamp());


-- ============================================================
-- STEP 8: VALIDATION - CHECK THE TABLE DATA
-- ============================================================
-- Tags do not change row data.
-- They are metadata labels, not data transformations.
-- So the table contents will look exactly the same.
-- ============================================================

SELECT *
FROM ecomsphere.gold.product_360_tag_demo
ORDER BY product_id;


-- ============================================================
-- STEP 9: VALIDATION - CHECK TAG REGISTRY TABLE
-- ============================================================
-- This is your own governance table for learning/reporting.
-- ============================================================

SELECT *
FROM ecomsphere.governance.asset_tag_registry
WHERE full_name = 'ecomsphere.gold.product_360_tag_demo'
ORDER BY column_name, tag_key;


-- ============================================================
-- STEP 10: OPTIONAL - REMOVE A TAG
-- ============================================================
-- If you want to practice changing metadata, you can remove a tag.
-- Uncomment only if you want to test removal.
-- ============================================================

-- UNSET TAG ON TABLE ecomsphere.gold.product_360_tag_demo `lifecycle_status`;

-- UNSET TAG ON COLUMN ecomsphere.gold.product_360_tag_demo.review_score `quality_status`;


-- ============================================================
-- STEP 11: IMPORTANT BEGINNER NOTES
-- ============================================================
--
-- 1. TAGS DO NOT FILTER DATA BY THEMSELVES
--    A tag is only metadata unless you connect it to a policy.
--
--    Example:
--      data_classification = confidential
--    does NOT automatically hide the data.
--
--    It only labels the data.
--
-- 2. GOVERNED TAGS ARE STRONGER THAN NORMAL TAGS
--    Governed tags are centrally controlled:
--      - allowed values are predefined
--      - only authorized users can assign them
--
-- 3. TAGS HELP PREPARE FOR ABAC
--    ABAC = Attribute-Based Access Control
--    In Databricks, governed tags can later drive centralized
--    masking and filtering policies.
--
-- 4. WHY THIS MATTERS IN INDUSTRY
--    Instead of manually remembering:
--      "this table is sensitive"
--    you formally label it with metadata.
--
--    That makes governance scalable.
--
-- ============================================================