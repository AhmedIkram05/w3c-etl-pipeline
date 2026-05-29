{{ config(materialized='table', tags=['mart', 'dbt']) }}

WITH page_stats AS (
    SELECT
        dp.page_sk,
        dp.page_path,
        dp.page_category,
        COUNT(*) AS total_requests,
        COUNT(DISTINCT fw.geolocation_sk) AS unique_hosts,
        SUM(CASE WHEN fw.is_404 THEN 1 ELSE 0 END) AS total_404,
        SUM(fw.bytes_sent) AS total_bytes_sent,
        AVG(fw.response_time_ms)::NUMERIC(10,2) AS avg_response_time_ms,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY fw.response_time_ms)::NUMERIC(10,2) AS p95_response_time_ms,
        MAX(fw.response_time_ms) AS max_response_time_ms,
        SUM(CASE WHEN fw.response_time_ms > 5000 THEN 1 ELSE 0 END) AS slow_requests,
        COUNT(DISTINCT fw.date_sk) AS active_days
    FROM {{ ref('fact_webrequest') }} fw
    JOIN {{ ref('dim_page') }} dp ON dp.page_sk = fw.page_sk
    GROUP BY dp.page_sk, dp.page_path, dp.page_category
)

SELECT
    page_sk,
    page_path,
    page_category,
    total_requests,
    unique_hosts,
    total_404,
    ROUND(100.0 * total_404 / NULLIF(total_requests, 0), 2) AS pct_404,
    total_bytes_sent,
    avg_response_time_ms,
    p95_response_time_ms,
    max_response_time_ms,
    slow_requests,
    ROUND(100.0 * slow_requests / NULLIF(total_requests, 0), 2) AS pct_slow_requests,
    active_days
FROM page_stats
