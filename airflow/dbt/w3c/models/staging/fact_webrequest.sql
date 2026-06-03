{{ config(
    materialized='incremental',
    unique_key='raw_log_id',
    on_schema_change='append_new_columns',
    tags=['fact', 'dbt'],
    post_hook=[
        "CREATE INDEX IF NOT EXISTS idx_fact_raw_log_id ON {{ this }}(raw_log_id)",
        "CREATE INDEX IF NOT EXISTS idx_fact_date_sk ON {{ this }}(date_sk)",
        "CREATE INDEX IF NOT EXISTS idx_fact_page_sk ON {{ this }}(page_sk)"
    ]
) }}

WITH enriched_input AS (
    SELECT
        *,
        -- Stable unique key (raw_log_id) generated since source has no auto-increment
        -- Includes enough dimensions to disambiguate concurrent identical-looking requests:
        -- source_file, log_time, client_ip, user_agent, referrer, uri_stem, uri_query,
        -- method, status, sub_status, win32_status, time_taken
        MD5(CONCAT(source_file, '|', log_time, '|', client_ip, '|', user_agent, '|', referrer,
                   '|', uri_stem, '|', uri_query, '|', method, '|', status, '|', sub_status, '|', win32_status, '|', time_taken)) AS raw_log_id,
        CASE WHEN status = 404 THEN TRUE ELSE FALSE END AS is_404,
        CASE WHEN referrer = '-' OR referrer IS NULL THEN TRUE ELSE FALSE END AS is_direct_traffic,
        1 AS request_count
    FROM {{ source('w3c', 'raw_enriched') }}
    {% if is_incremental() %}
    WHERE source_file NOT IN (SELECT DISTINCT source_file FROM {{ this }})
    {% endif %}
),

page_map AS (
    SELECT page_sk, page_path, query_string FROM {{ ref('dim_page') }}
),

status_map AS (
    SELECT status_sk, status_code, sub_status, win32_status FROM {{ ref('dim_status') }}
),

method_map AS (
    SELECT method_sk, http_method FROM {{ ref('dim_method') }}
),

referrer_map AS (
    SELECT referrer_sk, referrer_url FROM {{ ref('dim_referrer') }}
),

geo_map AS (
    SELECT
        geolocation_sk,
        ip AS client_ip
    FROM {{ source('w3c', 'dim_geolocation') }}
),

ua_map AS (
    SELECT user_agent_sk, user_agent FROM {{ source('w3c', 'dim_useragent') }}
),

ip_visit_buckets AS (
    SELECT
        client_ip,
        CASE
            WHEN COUNT(*) = 1 THEN '1 Visit'
            WHEN COUNT(*) <= 5 THEN '2-5 Visits'
            WHEN COUNT(*) <= 10 THEN '6-10 Visits'
            WHEN COUNT(*) <= 20 THEN '11-20 Visits'
            WHEN COUNT(*) <= 50 THEN '21-50 Visits'
            ELSE '51+ Visits'
        END AS visit_bucket_name
    FROM {{ source('w3c', 'raw_enriched') }}
    WHERE client_ip IS NOT NULL AND client_ip != '-'
    GROUP BY client_ip
),

computed AS (
    SELECT
        ei.raw_log_id,
        ei.source_file,
        ei.log_date::DATE AS log_date,
        ei.log_time,
        ei.client_ip,
        COALESCE(NULLIF(TRIM(ei.uri_stem), ''), 'Unknown') AS uri_stem,
        COALESCE(NULLIF(TRIM(ei.uri_query), ''), '-') AS uri_query,
        COALESCE(NULLIF(TRIM(UPPER(ei.method)), ''), 'Unknown') AS method_name,
        CASE WHEN ei.referrer IS NULL OR TRIM(ei.referrer) IN ('', '-') THEN 'Direct' ELSE TRIM(ei.referrer) END AS referrer_url,
        ei.user_agent,
        COALESCE(ei.status, -1) AS status_code,
        COALESCE(ei.sub_status, -1) AS sub_status,
        COALESCE(ei.win32_status, -1) AS win32_status,
        COALESCE(ei.bytes_sent, 0)::BIGINT AS bytes_sent,
        COALESCE(ei.bytes_recv, 0)::BIGINT AS bytes_received,
        COALESCE(ei.time_taken, 0)::INTEGER AS response_time_ms,
        ei.request_count,
        ei.is_404,
        -- is_crawler is now pre-computed in raw_enriched by the Spark pipeline
        COALESCE(ei.is_crawler, FALSE) AS is_crawler,
        ei.is_direct_traffic,
        -- size_band is now pre-computed in raw_enriched by the Spark pipeline
        COALESCE(ei.size_band, 'Zero') AS size_band,

        -- Computed enrichment (denormalized from raw_enriched)
        ei.page_category,
        ei.referrer_domain,
        ei.traffic_type,
        -- Visitor lookup key from is_crawler flag
        CASE WHEN COALESCE(ei.is_crawler, FALSE) THEN 1 ELSE 2 END AS visitor_key
    FROM enriched_input ei
)

SELECT
    c.raw_log_id,
    c.source_file,
    d.date_sk,
    t.time_sk,
    p.page_sk,
    m.method_sk,
    s.status_sk,
    rf.referrer_sk,
    COALESCE(g.geolocation_sk, -1) AS geolocation_sk,
    COALESCE(ua.user_agent_sk, -1) AS user_agent_sk,
    v.visitor_sk,
    vb.visit_bucket_sk,
    c.bytes_sent,
    c.bytes_received,
    c.response_time_ms,
    c.request_count,
    c.is_404,
    c.is_crawler,
    c.is_direct_traffic,
    c.size_band,

    c.page_category,
    c.referrer_domain,
    c.traffic_type,
    t.time_band
FROM (
    SELECT * FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY raw_log_id ORDER BY response_time_ms DESC) AS rn
        FROM computed
    ) t WHERE rn = 1
) c
INNER JOIN {{ ref('dim_date') }} d ON d.date = c.log_date
INNER JOIN {{ ref('dim_time') }} t
    ON t.hour = EXTRACT(HOUR FROM c.log_time::TIME)::INTEGER
    AND t.minute = EXTRACT(MINUTE FROM c.log_time::TIME)::INTEGER
INNER JOIN page_map p
    ON p.page_path = c.uri_stem
    AND p.query_string = c.uri_query
INNER JOIN method_map m ON m.http_method = c.method_name
INNER JOIN status_map s
    ON s.status_code = c.status_code
    AND s.sub_status = c.sub_status
    AND s.win32_status = c.win32_status
INNER JOIN referrer_map rf ON rf.referrer_url = c.referrer_url
LEFT JOIN geo_map g ON g.client_ip = c.client_ip
LEFT JOIN ua_map ua ON ua.user_agent = c.user_agent
INNER JOIN {{ ref('dim_visitortype') }} v ON v.visitor_sk = c.visitor_key
INNER JOIN ip_visit_buckets ib ON ib.client_ip = c.client_ip
INNER JOIN {{ ref('dim_visit_buckets') }} vb ON vb.visit_bucket = ib.visit_bucket_name
