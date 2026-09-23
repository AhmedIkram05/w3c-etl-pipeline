-- Singular test: verify dimensions have reasonable coverage
-- Each dbt-managed dimension should have rows

WITH dim_checks AS (
    SELECT 'dim_date' AS dim_name, COUNT(*) AS row_count FROM {{ ref('dim_date') }}
    UNION ALL
    SELECT 'dim_time', COUNT(*) FROM {{ ref('dim_time') }}
    UNION ALL
    SELECT 'dim_page', COUNT(*) FROM {{ ref('dim_page') }}
    UNION ALL
    SELECT 'dim_status', COUNT(*) FROM {{ ref('dim_status') }}
    UNION ALL
    SELECT 'dim_method', COUNT(*) FROM {{ ref('dim_method') }}
    UNION ALL
    SELECT 'dim_referrer', COUNT(*) FROM {{ ref('dim_referrer') }}
    UNION ALL
    SELECT 'dim_visit_buckets', COUNT(*) FROM {{ ref('dim_visit_buckets') }}
    UNION ALL
    SELECT 'dim_visitortype', COUNT(*) FROM {{ ref('dim_visitortype') }}
    UNION ALL
    SELECT 'crawler_ips', COUNT(*) FROM {{ ref('crawler_ips') }}
    UNION ALL
    SELECT 'fact_webrequest', COUNT(*) FROM {{ ref('fact_webrequest') }}
)

SELECT
    dim_name,
    row_count,
    CASE
        WHEN row_count = 0 THEN 'FAIL: Empty dimension'
        WHEN dim_name = 'dim_method' AND row_count < 3 THEN 'FAIL: Expected at least 3 HTTP methods'
        WHEN dim_name = 'dim_time' AND row_count <> 1440 THEN 'FAIL: Expected exactly 1440 time entries'
        WHEN dim_name = 'dim_visit_buckets' AND row_count < 3 THEN 'FAIL: Expected at least 3 visit buckets'
        WHEN dim_name = 'dim_visitortype' AND row_count <> 3 THEN 'FAIL: Expected exactly 3 visitor types'
        WHEN dim_name = 'crawler_ips' AND row_count = 0 THEN 'FAIL: Expected at least 1 crawler IP'
        ELSE 'PASS'
    END AS coverage_check
FROM dim_checks
WHERE row_count = 0
    OR (dim_name = 'dim_method' AND row_count < 3)
    OR (dim_name = 'dim_time' AND row_count <> 1440)
    OR (dim_name = 'dim_visit_buckets' AND row_count < 3)
    OR (dim_name = 'dim_visitortype' AND row_count <> 3)
    OR (dim_name = 'crawler_ips' AND row_count = 0)
