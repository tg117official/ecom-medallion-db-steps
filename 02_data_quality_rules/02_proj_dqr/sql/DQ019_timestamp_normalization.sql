-- ============================================================
-- DQ019 - Timestamp normalization
-- Standardization Template
-- ============================================================


CREATE OR REPLACE TEMP VIEW all_bronze_tables AS
SELECT * FROM VALUES (' sample ') AS t(raw_value);

SELECT *
FROM all_bronze_tables;

