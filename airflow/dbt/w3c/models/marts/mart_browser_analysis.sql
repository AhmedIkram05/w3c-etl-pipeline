{{ config(materialized='table', tags=['mart', 'dbt']) }}

WITH daily_total AS (
    SELECT
        fw.date_sk,
        dd.date,
        COUNT(*) AS daily_total_requests
    FROM {{ ref('fact_webrequest') }} fw
    JOIN {{ ref('dim_date') }} dd ON dd.date_sk = fw.date_sk
    GROUP BY fw.date_sk, dd.date
),

browser_stats AS (
    SELECT
        fw.date_sk,
        dd.date,
        ua.browser_name,
        ua.operating_system,
        ua.device_type,
        CASE WHEN ua.device_type = 'Mobile' THEN 1 ELSE 0 END AS is_mobile,
        COUNT(*) AS total_requests,
        COUNT(DISTINCT fw.geolocation_sk) AS unique_hosts,
        {{ tsql_cast('AVG(fw.response_time_ms)', 'NUMERIC(10,2)') }} AS avg_response_time_ms,
        SUM(fw.bytes_sent) AS total_bytes_sent
    FROM {{ ref('fact_webrequest') }} fw
    JOIN {{ ref('dim_date') }} dd ON dd.date_sk = fw.date_sk
    LEFT JOIN {{ source('w3c', 'dim_useragent') }} ua ON ua.user_agent_sk = fw.user_agent_sk
    WHERE fw.user_agent_sk > 0  -- exclude unknown user agents
    GROUP BY fw.date_sk, dd.date, ua.browser_name, ua.operating_system, ua.device_type
)

SELECT
    bs.date_sk,
    bs.date,
    bs.browser_name,
    bs.operating_system,
    bs.device_type,
    bs.is_mobile,
    CASE WHEN bs.device_type IN ('Desktop', 'Other') THEN 'Desktop' ELSE 'Mobile/Tablet' END AS device_category,
    bs.total_requests,
    bs.unique_hosts,
    bs.avg_response_time_ms,
    bs.total_bytes_sent,
    ROUND(100.0 * bs.total_requests / NULLIF(dt.daily_total_requests, 0), 2) AS pct_of_daily_traffic,
    ROW_NUMBER() OVER (PARTITION BY bs.date_sk ORDER BY bs.total_requests DESC) AS browser_rank
FROM browser_stats bs
JOIN daily_total dt ON dt.date_sk = bs.date_sk
