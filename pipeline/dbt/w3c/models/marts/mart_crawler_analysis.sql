{{ config(materialized='table', tags=['mart', 'dbt']) }}

WITH crawler_requests AS (
    SELECT
        fw.date_sk,
        dd.date,
        fw.page_sk,
        dp.page_path,
        dp.page_category,
        fw.status_sk,
        ds.status_category,
        fw.geolocation_sk,
        fw.bytes_sent,
        fw.response_time_ms
    FROM {{ ref('fact_webrequest') }} fw
    JOIN {{ ref('dim_date') }} dd ON dd.date_sk = fw.date_sk
    JOIN {{ ref('dim_page') }} dp ON dp.page_sk = fw.page_sk
    JOIN {{ ref('dim_status') }} ds ON ds.status_sk = fw.status_sk
    WHERE fw.is_crawler = {{ tsql_true_val() }}
)

SELECT
    date_sk,
    date,
    COUNT(*) AS total_crawler_requests,
    COUNT(DISTINCT page_sk) AS distinct_pages_hit,
    COUNT(DISTINCT geolocation_sk) AS distinct_hosts_hit,
    SUM(bytes_sent) AS total_bytes_transferred,
    ROUND({{ tsql_cast('SUM(bytes_sent)', 'NUMERIC') }} / NULLIF(COUNT(*), 0), 2) AS avg_bytes_per_request,
    {{ tsql_cast('AVG(response_time_ms)', 'NUMERIC(10,2)') }} AS avg_response_time_ms,
    MAX(response_time_ms) AS max_response_time_ms,
    COUNT(DISTINCT CASE WHEN status_category = 'Client Error' THEN page_sk END) AS pages_with_errors,
    ROUND(100.0 * SUM(CASE WHEN status_category IN ('Client Error', 'Server Error') THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS pct_error_responses
FROM crawler_requests
GROUP BY date_sk, date
