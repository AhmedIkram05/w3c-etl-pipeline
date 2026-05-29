-- Singular test: verify fact_webrequest has no orphan foreign keys
-- All FK values should resolve to existing dimensions

WITH fk_checks AS (
    SELECT
        'date_sk' AS fk_name,
        COUNT(*) AS total,
        SUM(CASE WHEN date_sk != -1 AND date_sk NOT IN (SELECT date_sk FROM {{ ref('dim_date') }}) THEN 1 ELSE 0 END) AS orphans
    FROM {{ ref('fact_webrequest') }}
    UNION ALL
    SELECT
        'time_sk',
        COUNT(*),
        SUM(CASE WHEN time_sk != -1 AND time_sk NOT IN (SELECT time_sk FROM {{ ref('dim_time') }}) THEN 1 ELSE 0 END)
    FROM {{ ref('fact_webrequest') }}
    UNION ALL
    SELECT
        'page_sk',
        COUNT(*),
        SUM(CASE WHEN page_sk != -1 AND page_sk NOT IN (SELECT page_sk FROM {{ ref('dim_page') }}) THEN 1 ELSE 0 END)
    FROM {{ ref('fact_webrequest') }}
    UNION ALL
    SELECT
        'method_sk',
        COUNT(*),
        SUM(CASE WHEN method_sk != -1 AND method_sk NOT IN (SELECT method_sk FROM {{ ref('dim_method') }}) THEN 1 ELSE 0 END)
    FROM {{ ref('fact_webrequest') }}
    UNION ALL
    SELECT
        'status_sk',
        COUNT(*),
        SUM(CASE WHEN status_sk != -1 AND status_sk NOT IN (SELECT status_sk FROM {{ ref('dim_status') }}) THEN 1 ELSE 0 END)
    FROM {{ ref('fact_webrequest') }}
    UNION ALL
    SELECT
        'referrer_sk',
        COUNT(*),
        SUM(CASE WHEN referrer_sk != -1 AND referrer_sk NOT IN (SELECT referrer_sk FROM {{ ref('dim_referrer') }}) THEN 1 ELSE 0 END)
    FROM {{ ref('fact_webrequest') }}
    UNION ALL
    SELECT
        'visit_bucket_sk',
        COUNT(*),
        SUM(CASE WHEN visit_bucket_sk != -1 AND visit_bucket_sk NOT IN (SELECT visit_bucket_sk FROM {{ ref('dim_visit_buckets') }}) THEN 1 ELSE 0 END)
    FROM {{ ref('fact_webrequest') }}
    UNION ALL
    SELECT
        'geolocation_sk',
        COUNT(*),
        SUM(CASE WHEN geolocation_sk != -1 AND geolocation_sk NOT IN (SELECT geolocation_sk FROM {{ source('w3c', 'dim_geolocation') }}) THEN 1 ELSE 0 END)
    FROM {{ ref('fact_webrequest') }}
    UNION ALL
    SELECT
        'user_agent_sk',
        COUNT(*),
        SUM(CASE WHEN user_agent_sk != -1 AND user_agent_sk NOT IN (SELECT user_agent_sk FROM {{ source('w3c', 'dim_useragent') }}) THEN 1 ELSE 0 END)
    FROM {{ ref('fact_webrequest') }}
    UNION ALL
    SELECT
        'visitor_sk',
        COUNT(*),
        SUM(CASE WHEN visitor_sk != -1 AND visitor_sk NOT IN (SELECT visitor_sk FROM {{ ref('dim_visitortype') }}) THEN 1 ELSE 0 END)
    FROM {{ ref('fact_webrequest') }}
)

SELECT
    fk_name,
    total,
    orphans,
    ROUND(100.0 * orphans / NULLIF(total, 0), 2) AS orphan_pct
FROM fk_checks
WHERE orphans > 0
ORDER BY orphans DESC
