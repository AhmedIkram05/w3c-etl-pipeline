{{ config(materialized='table', tags=['mart', 'dbt']) }}

WITH page_stats AS (
    SELECT
        dp.page_sk,
        dp.page_path,
        dp.page_category,
        COUNT(*) AS total_requests,
        COUNT(DISTINCT fw.geolocation_sk) AS unique_hosts,
        SUM({{ tsql_boolean_to_int('fw.is_404') }}) AS total_404,
        SUM(fw.bytes_sent) AS total_bytes_sent,
        {{ tsql_cast('AVG(fw.response_time_ms)', 'NUMERIC(10,2)') }} AS avg_response_time_ms,
        MAX(fw.response_time_ms) AS max_response_time_ms,
        SUM(CASE WHEN fw.response_time_ms > 5000 THEN 1 ELSE 0 END) AS slow_requests,
        COUNT(DISTINCT fw.date_sk) AS active_days
    FROM {{ ref('fact_webrequest') }} fw
    JOIN {{ ref('dim_page') }} dp ON dp.page_sk = fw.page_sk
    GROUP BY dp.page_sk, dp.page_path, dp.page_category
),
p95_cte AS (
    {% if target.type == 'sqlserver' %}
    SELECT DISTINCT
        fw.page_sk,
        {{ tsql_cast(
            'PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY fw.response_time_ms) OVER (PARTITION BY fw.page_sk)',
            'NUMERIC(10,2)'
        ) }} AS p95_response_time_ms
    FROM {{ ref('fact_webrequest') }} fw
    {% else %}
    SELECT
        fw.page_sk,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY fw.response_time_ms)::NUMERIC(10,2) AS p95_response_time_ms
    FROM {{ ref('fact_webrequest') }} fw
    GROUP BY fw.page_sk
    {% endif %}
)

SELECT
    ps.page_sk,
    ps.page_path,
    ps.page_category,
    ps.total_requests,
    ps.unique_hosts,
    ps.total_404,
    ROUND(100.0 * ps.total_404 / NULLIF(ps.total_requests, 0), 2) AS pct_404,
    ps.total_bytes_sent,
    ps.avg_response_time_ms,
    p95.p95_response_time_ms,
    max_response_time_ms,
    ps.slow_requests,
    ROUND(100.0 * ps.slow_requests / NULLIF(ps.total_requests, 0), 2) AS pct_slow_requests,
    ps.active_days
FROM page_stats ps
LEFT JOIN p95_cte p95 ON p95.page_sk = ps.page_sk
