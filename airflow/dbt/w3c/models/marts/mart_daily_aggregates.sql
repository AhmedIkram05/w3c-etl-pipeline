{{ config(materialized='table', tags=['mart', 'dbt']) }}

WITH daily_stats AS (
    SELECT
        fw.date_sk,
        dd.date,
        dd.day_name,
        dd.is_weekend,
        dd.holiday_flag,
        dd.year,
        dd.month,
        COUNT(*) AS total_requests,
        COUNT(DISTINCT fw.geolocation_sk) AS unique_hosts,
        COUNT(DISTINCT CASE WHEN fw.is_crawler THEN NULL ELSE fw.geolocation_sk END) AS unique_human_hosts,
        COUNT(DISTINCT fw.page_sk) AS unique_pages,
        COUNT(DISTINCT CASE WHEN fw.geolocation_sk > 0 THEN dg.country END) AS active_countries,
        SUM(fw.is_404::INT) AS total_404,
        AVG(fw.response_time_ms)::NUMERIC(10,2) AS avg_response_time_ms,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY fw.response_time_ms)::NUMERIC(10,2) AS p95_response_time_ms,
        SUM(fw.bytes_sent) AS total_bytes_sent,
        SUM(CASE WHEN fw.is_crawler THEN 1 ELSE 0 END) AS crawler_requests,
        SUM(CASE WHEN fw.is_direct_traffic THEN 1 ELSE 0 END) AS direct_traffic_requests,
        SUM(CASE WHEN fw.response_time_ms > 5000 THEN 1 ELSE 0 END) AS slow_requests
    FROM {{ ref('fact_webrequest') }} fw
    JOIN {{ ref('dim_date') }} dd ON dd.date_sk = fw.date_sk
    LEFT JOIN {{ source('w3c', 'dim_geolocation') }} dg ON dg.geolocation_sk = fw.geolocation_sk
    GROUP BY fw.date_sk, dd.date, dd.day_name, dd.is_weekend, dd.holiday_flag, dd.year, dd.month
)

SELECT
    date_sk,
    date,
    day_name,
    is_weekend,
    holiday_flag,
    year,
    month,
    total_requests,
    unique_hosts,
    unique_human_hosts,
    unique_pages,
    active_countries,
    total_404,
    ROUND(100.0 * total_404 / NULLIF(total_requests, 0), 2) AS pct_404,
    avg_response_time_ms,
    p95_response_time_ms,
    total_bytes_sent,
    crawler_requests,
    ROUND(100.0 * crawler_requests / NULLIF(total_requests, 0), 2) AS pct_crawler,
    direct_traffic_requests,
    slow_requests,
    ROUND(100.0 * slow_requests / NULLIF(total_requests, 0), 2) AS pct_slow_requests
FROM daily_stats
