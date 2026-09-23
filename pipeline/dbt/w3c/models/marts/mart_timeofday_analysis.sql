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
        SUM({{ tsql_boolean_to_int('fw.is_404') }}) AS total_404,
        {{ tsql_cast('AVG(fw.response_time_ms)', 'NUMERIC(10,2)') }} AS avg_response_time_ms,
        SUM(fw.bytes_sent) AS total_bytes_sent,
        SUM({{ tsql_boolean_to_int('fw.is_crawler') }}) AS crawler_requests,
        SUM(CASE WHEN fw.response_time_ms > 5000 THEN 1 ELSE 0 END) AS slow_requests
    FROM {{ ref('fact_webrequest') }} fw
    JOIN {{ ref('dim_date') }} dd ON dd.date_sk = fw.date_sk
    JOIN {{ ref('dim_time') }} dt ON dt.time_sk = fw.time_sk
    GROUP BY fw.date_sk, dd.date, dd.day_name, dt.hour, dt.time_band
),
p95_cte AS (
    {% if target.type == 'sqlserver' %}
    SELECT DISTINCT
        fw.date_sk,
        dt.hour,
        {{ tsql_cast(
            'PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY fw.response_time_ms) OVER (PARTITION BY fw.date_sk, dt.hour)',
            'NUMERIC(10,2)'
        ) }} AS p95_response_time_ms
    FROM {{ ref('fact_webrequest') }} fw
    JOIN {{ ref('dim_time') }} dt ON dt.time_sk = fw.time_sk
    {% else %}
    SELECT
        fw.date_sk,
        dt.hour,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY fw.response_time_ms)::NUMERIC(10,2) AS p95_response_time_ms
    FROM {{ ref('fact_webrequest') }} fw
    JOIN {{ ref('dim_time') }} dt ON dt.time_sk = fw.time_sk
    GROUP BY fw.date_sk, dt.hour
    {% endif %}
)

SELECT
    hs.date_sk,
    hs.date,
    hs.day_name,
    hs.hour,
    hs.time_band,
    hs.total_requests,
    hs.unique_pages,
    hs.unique_hosts,
    hs.total_404,
    ROUND(100.0 * hs.total_404 / NULLIF(hs.total_requests, 0), 2) AS pct_404,
    hs.avg_response_time_ms,
    p95.p95_response_time_ms,
    total_bytes_sent,
    crawler_requests,
    ROUND(100.0 * crawler_requests / NULLIF(total_requests, 0), 2) AS pct_crawler,
    hs.slow_requests,
    ROUND(100.0 * hs.slow_requests / NULLIF(hs.total_requests, 0), 2) AS pct_slow_requests
FROM hourly_stats hs
LEFT JOIN p95_cte p95 ON p95.date_sk = hs.date_sk AND p95.hour = hs.hour
