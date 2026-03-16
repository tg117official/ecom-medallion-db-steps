-- ============================================================
-- DQ006 - Record-level schema conformance
-- Record-Level Schema Conformance Template
-- ============================================================

WITH src AS (
    SELECT * FROM VALUES
    ('R001', 'O1001', 'C101', 'P101', 'PV101', '5', '2026-03-10 10:00:00'),
    ('R002', 'O1002', 'C102', 'P102', 'PV102', 'bad_rating', 'bad_ts'),
    (NULL, 'O1003', 'C103', 'P103', 'PV103', '3', '2026-03-10 10:30:00')
    AS t(review_id, order_id, customer_id, product_id, product_variant_id, rating_raw, review_created_at_raw)
),
conformed AS (
    SELECT *,
           TRY_CAST(rating_raw AS INT) AS rating,
           TRY_CAST(review_created_at_raw AS TIMESTAMP) AS review_created_at
    FROM src
)
SELECT *,
       CASE
           WHEN review_id IS NULL
             OR order_id IS NULL
             OR customer_id IS NULL
             OR product_id IS NULL
             OR product_variant_id IS NULL
             OR rating IS NULL
             OR review_created_at IS NULL
           THEN 1 ELSE 0
       END AS dq_schema_conformance_flag
FROM conformed;