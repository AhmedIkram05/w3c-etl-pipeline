{{ config(materialized='table', tags=['mart', 'dbt']) }}

WITH country_browser_stats AS (
    SELECT
        fw.date_sk,
        dd.date,
        dg.country,
        ua.browser_name,
        COUNT(*) AS total_requests,
        SUM(COUNT(*)) OVER (
            PARTITION BY fw.date_sk, dg.country
        ) AS daily_country_total,
        ROW_NUMBER() OVER (
            PARTITION BY fw.date_sk, dg.country
            ORDER BY COUNT(*) DESC
        ) AS browser_rank
    FROM {{ ref('fact_webrequest') }} fw
    JOIN {{ ref('dim_date') }} dd ON dd.date_sk = fw.date_sk
    LEFT JOIN {{ source('w3c', 'dim_geolocation') }} dg
        ON dg.geolocation_sk = fw.geolocation_sk
    LEFT JOIN {{ source('w3c', 'dim_useragent') }} ua
        ON ua.user_agent_sk = fw.user_agent_sk
    WHERE fw.geolocation_sk > 0
      AND ua.user_agent_sk > 0
      AND dg.country IS NOT NULL
    GROUP BY fw.date_sk, dd.date, dg.country, ua.browser_name
)

SELECT
    date_sk,
    date,
    country,
    browser_name AS top_browser,
    total_requests AS top_browser_requests,
    ROUND(100.0 * total_requests / NULLIF(daily_country_total, 0), 2) AS top_browser_share_pct
FROM country_browser_stats
WHERE browser_rank = 1
