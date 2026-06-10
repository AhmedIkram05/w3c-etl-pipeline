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
        COUNT(DISTINCT CASE
            {% if target.type == 'sqlserver' %}
                WHEN fw.is_crawler = 1 THEN NULL
            {% else %}
                WHEN fw.is_crawler THEN NULL
            {% endif %}
            ELSE fw.geolocation_sk END) AS unique_human_hosts,
        COUNT(DISTINCT fw.page_sk) AS unique_pages,
        COUNT(DISTINCT CASE WHEN fw.geolocation_sk > 0 THEN dg.country END) AS active_countries,
        SUM({{ tsql_boolean_to_int('fw.is_404') }}) AS total_404,
        {{ tsql_cast('AVG(fw.response_time_ms)', 'NUMERIC(10,2)') }} AS avg_response_time_ms,
        {% if target.type == 'sqlserver' %}
            -- PERCENTILE_CONT not supported in GROUP BY context on SQL Server
            NULL AS p95_response_time_ms,
        {% else %}
            PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY fw.response_time_ms)::NUMERIC(10,2) AS p95_response_time_ms,
        {% endif %}
        SUM(fw.bytes_sent) AS total_bytes_sent,
        SUM({{ tsql_boolean_to_int('fw.is_crawler') }}) AS crawler_requests,
        SUM({{ tsql_boolean_to_int('fw.is_direct_traffic') }}) AS direct_traffic_requests,
        SUM(CASE WHEN fw.response_time_ms > 5000 THEN 1 ELSE 0 END) AS slow_requests
    FROM {{ ref('fact_webrequest') }} fw
    JOIN {{ ref('dim_date') }} dd ON dd.date_sk = fw.date_sk
    LEFT JOIN {{ source('w3c', 'dim_geolocation') }} dg ON dg.geolocation_sk = fw.geolocation_sk
    GROUP BY fw.date_sk, dd.date, dd.day_name, dd.is_weekend, dd.holiday_flag, dd.year, dd.month
),
peak_hour AS (
    SELECT
        date_sk,
        hour AS peak_traffic_hour,
        total_requests AS peak_hour_requests
    FROM (
        SELECT
            date_sk,
            hour,
            total_requests,
            ROW_NUMBER() OVER (PARTITION BY date_sk ORDER BY total_requests DESC) AS rn
        FROM {{ ref('mart_timeofday_analysis') }}
    ) ranked
    WHERE rn = 1
),
top_browser AS (
    SELECT
        date_sk,
        pct_of_daily_traffic AS top_browser_share
    FROM {{ ref('mart_browser_analysis') }}
    WHERE browser_rank = 1
)

SELECT
    ds.date_sk,
    ds.date,
    ds.day_name,
    ds.is_weekend,
    ds.holiday_flag,
    ds.year,
    ds.month,
    ds.total_requests,
    ds.unique_hosts,
    ds.unique_human_hosts,
    ds.unique_pages,
    ds.active_countries,
    ds.total_404,
    ROUND(100.0 * ds.total_404 / NULLIF(ds.total_requests, 0), 2) AS pct_404,
    ds.avg_response_time_ms,
    ds.p95_response_time_ms,
    ds.total_bytes_sent,
    ds.crawler_requests,
    ROUND(100.0 * ds.crawler_requests / NULLIF(ds.total_requests, 0), 2) AS pct_crawler,
    ds.direct_traffic_requests,
    ds.slow_requests,
    ROUND(100.0 * ds.slow_requests / NULLIF(ds.total_requests, 0), 2) AS pct_slow_requests,
    ph.peak_hour_requests,
    ph.peak_traffic_hour,
    tb.top_browser_share
FROM daily_stats ds
LEFT JOIN peak_hour ph ON ph.date_sk = ds.date_sk
LEFT JOIN top_browser tb ON tb.date_sk = ds.date_sk
