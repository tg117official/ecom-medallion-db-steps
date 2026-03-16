-- ============================================================
-- DQ035 - Inventory consistency validation
-- Range / Domain Validation Template
-- ============================================================

WITH src AS (
    SELECT * FROM VALUES
    ('W1','PV1',10,2), ('W2','PV2',5,8), ('W3','PV3',7,7)
    AS t(warehouse_id, product_variant_id, on_hand_qty, reserved_qty)
)
SELECT *,
       on_hand_qty - reserved_qty AS available_qty,
       CASE WHEN reserved_qty > on_hand_qty OR (on_hand_qty - reserved_qty) < 0 THEN 1 ELSE 0 END AS dq_negative_stock_flag
FROM src;
