-- ============================================================
-- DQ034 - Rating range validation
-- Range / Domain Validation Template
-- ============================================================

WITH src AS (
    SELECT * FROM VALUES
    ('R1',5), ('R2',0), ('R3',6), ('R4',3)
    AS t(review_id, rating)
)
SELECT *,
       CASE WHEN rating < 1 OR rating > 5 THEN 1 ELSE 0 END AS dq_range_violation_flag
FROM src;
