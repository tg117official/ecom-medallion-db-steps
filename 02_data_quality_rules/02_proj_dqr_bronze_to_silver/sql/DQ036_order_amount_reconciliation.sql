-- ============================================================
-- DQ036 - Order amount reconciliation
-- Range / Domain Validation Template
-- ============================================================

WITH src AS (
    SELECT * FROM VALUES
    ('O1',100.0,10.0,5.0,20.0,115.0),
    ('O2',100.0,10.0,5.0,20.0,120.0)
    AS t(order_id, total_item_amount, total_discount_amount, total_tax_amount, total_shipping_amount, grand_total_amount)
)
SELECT *,
       total_item_amount - total_discount_amount + total_tax_amount + total_shipping_amount AS expected_grand_total,
       CASE WHEN ABS((total_item_amount - total_discount_amount + total_tax_amount + total_shipping_amount) - grand_total_amount) > 0.01 THEN 1 ELSE 0 END AS dq_amount_mismatch_flag
FROM src;
