-- ============================================================
-- DQ033 - Monetary amount must be non-negative
-- Range / Domain Validation Template
-- ============================================================

WITH src AS (
    SELECT * FROM VALUES
    ('A1',100.0,10.0,5.0,20.0),
    ('A2',-1.0,10.0,5.0,20.0),
    ('A3',100.0,-2.0,5.0,-1.0)
    AS t(id, amount1, amount2, amount3, amount4)
)
SELECT *,
       CASE WHEN amount1 < 0 OR amount2 < 0 OR amount3 < 0 OR amount4 < 0 THEN 1 ELSE 0 END AS dq_range_violation_flag
FROM src;
