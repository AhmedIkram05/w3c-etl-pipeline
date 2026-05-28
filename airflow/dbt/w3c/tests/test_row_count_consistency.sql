-- Singular test: verify fact_webrequest row count matches reality
-- The fact should have rows within reason given the known data volume (155K requests)

WITH fact_stats AS (
    SELECT
        COUNT(*) AS total_rows,
        COUNT(DISTINCT raw_log_id) AS unique_log_ids,
        COUNT(DISTINCT date_sk) AS distinct_dates,
        MIN(raw_log_id) AS min_id,
        MAX(raw_log_id) AS max_id
    FROM {{ ref('fact_webrequest') }}
)

SELECT
    total_rows,
    unique_log_ids,
    distinct_dates,
    min_id,
    max_id,
    CASE
        WHEN total_rows = 0 THEN 'FAIL: Fact table is empty'
        WHEN unique_log_ids < total_rows THEN 'FAIL: Duplicate raw_log_id values found'
        WHEN distinct_dates < 1 THEN 'FAIL: No dates in fact table'
        ELSE 'PASS'
    END AS row_check
FROM fact_stats
WHERE total_rows = 0
    OR unique_log_ids < total_rows
    OR distinct_dates < 1
