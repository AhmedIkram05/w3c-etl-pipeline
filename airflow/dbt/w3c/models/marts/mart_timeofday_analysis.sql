{{ config(materialized='table', tags=['mart', 'dbt']) }}

WITH hourly_stats AS (
    SELECT
        fw.date_sk,
        dd.date,
        dd.day_name,
        dt.hour,
        dt.time_band,
        COUNT(*) AS total_requests,
        COUNT(DISTINCT fw.page_sk) AS unique_pages,
        COUNT(DISTINCT fw.geolocation_sk) AS unique_hosts,
        SUM(fw.is_404::INT) AS total_404,
        AVG(fw.response_time_ms)::NUMERIC(10,2) AS avg_response_time_ms,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY fw.response_time_ms)::NUMERIC(10,2) AS p95_response_time_ms,
        SUM(fw.bytes_sent) AS total_bytes_sent,
        SUM(CASE WHEN fw.is_crawler THEN 1 ELSE 0 END) AS crawler_requests,
        SUM(CASE WHEN fw.response_time_ms > 5000 THEN 1 ELSE 0 END) AS slow_requests
    FROM {{ ref('fact_webrequest') }} fw
    JOIN {{ ref('dim_date') }} dd ON dd.date_sk = fw.date_sk
    JOIN {{ ref('dim_time') }} dt ON dt.time_sk = fw.time_sk
    GROUP BY fw.date_sk, dd.date, dd.day_name, dt.hour, dt.time_band
)

SELECT
    date_sk,
    date,
    day_name,
    hour,
    time_band,
    total_requests,
    unique_pages,
    unique_hosts,
    total_404,
    ROUND(100.0 * total_404 / NULLIF(total_requests, 0), 2) AS pct_404,
    avg_response_time_ms,
    p95_response_time_ms,
    total_bytes_sent,
    crawler_requests,
    ROUND(100.0 * crawler_requests / NULLIF(total_requests, 0), 2) AS pct_crawler,
    slow_requests,
    ROUND(100.0 * slow_requests / NULLIF(total_requests, 0), 2) AS pct_slow_requests
FROM hourly_stats
