-- ============================================================
-- DQ005 - Column datatype compatibility check
-- Datatype Compatibility Check Template
-- ============================================================

WITH src AS (
    SELECT * FROM VALUES
    ('amit@gmail.com', '2026-03-10 09:00:00', '2026-03-10 10:00:00', '2', '215.50', '115.25', '5', '2026-03-10 11:00:00', '4200.00'),
    ('bad_email', 'bad_ts', 'bad_ts', 'abc', 'xyz', '-10', '9', 'not_ts', 'text')
    AS t(email, customer_created_at, order_date, quantity, line_total_amount, payment_amount, rating, event_timestamp, selling_price)
)
SELECT *,
       CASE WHEN TRY_CAST(customer_created_at AS TIMESTAMP) IS NOT NULL THEN true ELSE false END AS customer_created_at_cast_ok,
       CASE WHEN TRY_CAST(order_date AS TIMESTAMP) IS NOT NULL THEN true ELSE false END AS order_date_cast_ok,
       CASE WHEN TRY_CAST(quantity AS INT) IS NOT NULL THEN true ELSE false END AS quantity_cast_ok,
       CASE WHEN TRY_CAST(line_total_amount AS DOUBLE) IS NOT NULL THEN true ELSE false END AS line_total_amount_cast_ok,
       CASE WHEN TRY_CAST(payment_amount AS DOUBLE) IS NOT NULL THEN true ELSE false END AS payment_amount_cast_ok,
       CASE WHEN TRY_CAST(rating AS INT) IS NOT NULL THEN true ELSE false END AS rating_cast_ok,
       CASE WHEN TRY_CAST(event_timestamp AS TIMESTAMP) IS NOT NULL THEN true ELSE false END AS event_timestamp_cast_ok,
       CASE WHEN TRY_CAST(selling_price AS DOUBLE) IS NOT NULL THEN true ELSE false END AS selling_price_cast_ok
FROM src;