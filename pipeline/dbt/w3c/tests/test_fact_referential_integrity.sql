-- Singular test: verify fact_webrequest has no orphan foreign keys
-- All FK values should resolve to existing dimensions
-- Uses LEFT JOIN instead of NOT IN subqueries for SQL Server compatibility

WITH fk_checks AS (
    SELECT
        'date_sk' AS fk_name,
        COUNT(*) AS total,
        COUNT(CASE WHEN f.date_sk != -1 AND d.date_sk IS NULL THEN 1 END) AS orphans
    FROM {{ ref('fact_webrequest') }} f
    LEFT JOIN {{ ref('dim_date') }} d ON f.date_sk = d.date_sk
    UNION ALL
    SELECT
        'time_sk',
        COUNT(*),
        COUNT(CASE WHEN f.time_sk != -1 AND d.time_sk IS NULL THEN 1 END)
    FROM {{ ref('fact_webrequest') }} f
    LEFT JOIN {{ ref('dim_time') }} d ON f.time_sk = d.time_sk
    UNION ALL
    SELECT
        'page_sk',
        COUNT(*),
        COUNT(CASE WHEN f.page_sk != -1 AND d.page_sk IS NULL THEN 1 END)
    FROM {{ ref('fact_webrequest') }} f
    LEFT JOIN {{ ref('dim_page') }} d ON f.page_sk = d.page_sk
    UNION ALL
    SELECT
        'method_sk',
        COUNT(*),
        COUNT(CASE WHEN f.method_sk != -1 AND d.method_sk IS NULL THEN 1 END)
    FROM {{ ref('fact_webrequest') }} f
    LEFT JOIN {{ ref('dim_method') }} d ON f.method_sk = d.method_sk
    UNION ALL
    SELECT
        'status_sk',
        COUNT(*),
        COUNT(CASE WHEN f.status_sk != -1 AND d.status_sk IS NULL THEN 1 END)
    FROM {{ ref('fact_webrequest') }} f
    LEFT JOIN {{ ref('dim_status') }} d ON f.status_sk = d.status_sk
    UNION ALL
    SELECT
        'referrer_sk',
        COUNT(*),
        COUNT(CASE WHEN f.referrer_sk != -1 AND d.referrer_sk IS NULL THEN 1 END)
    FROM {{ ref('fact_webrequest') }} f
    LEFT JOIN {{ ref('dim_referrer') }} d ON f.referrer_sk = d.referrer_sk
    UNION ALL
    SELECT
        'visit_bucket_sk',
        COUNT(*),
        COUNT(CASE WHEN f.visit_bucket_sk != -1 AND d.visit_bucket_sk IS NULL THEN 1 END)
    FROM {{ ref('fact_webrequest') }} f
    LEFT JOIN {{ ref('dim_visit_buckets') }} d ON f.visit_bucket_sk = d.visit_bucket_sk
    UNION ALL
    SELECT
        'geolocation_sk',
        COUNT(*),
        COUNT(CASE WHEN f.geolocation_sk != -1 AND d.geolocation_sk IS NULL THEN 1 END)
    FROM {{ ref('fact_webrequest') }} f
    LEFT JOIN {{ source('w3c', 'dim_geolocation') }} d ON f.geolocation_sk = d.geolocation_sk
    UNION ALL
    SELECT
        'user_agent_sk',
        COUNT(*),
        COUNT(CASE WHEN f.user_agent_sk != -1 AND d.user_agent_sk IS NULL THEN 1 END)
    FROM {{ ref('fact_webrequest') }} f
    LEFT JOIN {{ source('w3c', 'dim_useragent') }} d ON f.user_agent_sk = d.user_agent_sk
    UNION ALL
    SELECT
        'visitor_sk',
        COUNT(*),
        COUNT(CASE WHEN f.visitor_sk != -1 AND d.visitor_sk IS NULL THEN 1 END)
    FROM {{ ref('fact_webrequest') }} f
    LEFT JOIN {{ ref('dim_visitortype') }} d ON f.visitor_sk = d.visitor_sk
)

SELECT
    fk_name,
    total,
    orphans,
    ROUND(100.0 * orphans / NULLIF(total, 0), 2) AS orphan_pct
FROM fk_checks
WHERE orphans > 0
