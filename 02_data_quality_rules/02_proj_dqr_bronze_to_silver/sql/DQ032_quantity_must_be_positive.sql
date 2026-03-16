-- ============================================================
-- DQ032 - Quantity must be positive
-- Range / Domain Validation Template
-- ============================================================

WITH src AS (
    SELECT * FROM VALUES
    ('OI101',2,10,2),
    ('OI102',0,5,1),
    ('OI103',-1,8,-2)
    AS t(id, quantity, on_hand_qty, reserved_qty)
)
SELECT *,
       CASE WHEN quantity <= 0 OR on_hand_qty < 0 OR reserved_qty < 0 THEN 1 ELSE 0 END AS dq_range_violation_flag
FROM src;
