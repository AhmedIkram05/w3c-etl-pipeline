{{ config(
    materialized='table',
    tags=['fact', 'dbt'],
    post_hook=[
        "CREATE INDEX IF NOT EXISTS idx_fact_raw_log_id ON {{ this }}(raw_log_id)",
        "CREATE INDEX IF NOT EXISTS idx_fact_date_sk ON {{ this }}(date_sk)",
        "CREATE INDEX IF NOT EXISTS idx_fact_page_sk ON {{ this }}(page_sk)",
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_fact_raw_log_id_unique ON {{ this }}(raw_log_id)"
    ]
) }}

WITH raw AS (
    SELECT
        rl.id AS raw_log_id,
        rl.log_date,
        rl.log_time,
        rl.client_ip,
        rl.uri_stem,
        rl.uri_query,
        rl.method,
        rl.referrer,
        rl.user_agent,
        rl.status,
        rl.sub_status,
        rl.win32_status,
        rl.bytes_sent,
        rl.bytes_recv,
        rl.time_taken,
        rl.source_file
    FROM {{ source('w3c', 'raw_logs') }} rl
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
    SELECT geolocation_sk, ip AS client_ip FROM {{ source('w3c', 'dim_geolocation') }}
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
    FROM {{ source('w3c', 'raw_logs') }}
    WHERE client_ip IS NOT NULL AND client_ip != '-'
    GROUP BY client_ip
),

crawler_ips_list AS (
    SELECT ip FROM {{ source('w3c', 'crawler_ips') }}
),

computed AS (
    SELECT
        r.raw_log_id,
        r.source_file,
        r.log_date::DATE AS log_date,
        r.log_time,
        r.client_ip,
        COALESCE(NULLIF(TRIM(r.uri_stem), ''), 'Unknown') AS uri_stem,
        COALESCE(NULLIF(TRIM(r.uri_query), ''), '-') AS uri_query,
        COALESCE(NULLIF(TRIM(UPPER(r.method)), ''), 'Unknown') AS method_name,
        CASE WHEN r.referrer IS NULL OR TRIM(r.referrer) IN ('', '-') THEN 'Direct' ELSE TRIM(r.referrer) END AS referrer_url,
        r.user_agent,
        COALESCE(r.status, -1) AS status_code,
        COALESCE(r.sub_status, -1) AS sub_status,
        COALESCE(r.win32_status, -1) AS win32_status,
        COALESCE(r.bytes_sent, 0)::BIGINT AS bytes_sent,
        COALESCE(r.bytes_recv, 0)::BIGINT AS bytes_received,
        COALESCE(r.time_taken, 0)::INTEGER AS response_time_ms,
        1 AS request_count,
        -- Computed flags
        CASE WHEN r.status = 404 THEN TRUE ELSE FALSE END AS is_404,
        CASE WHEN r.client_ip IN (
            SELECT ip FROM crawler_ips_list
        ) THEN TRUE ELSE FALSE END AS is_crawler,
        CASE WHEN r.referrer IS NULL OR TRIM(r.referrer) = '' OR TRIM(r.referrer) = '-'
            THEN TRUE ELSE FALSE END AS is_direct_traffic,
        -- Size band (based on total bytes sent + received)
        CASE
            WHEN COALESCE(r.bytes_sent, 0) + COALESCE(r.bytes_recv, 0) = 0 THEN 'Zero'
            WHEN COALESCE(r.bytes_sent, 0) + COALESCE(r.bytes_recv, 0) < 1024 THEN 'Tiny'
            WHEN COALESCE(r.bytes_sent, 0) + COALESCE(r.bytes_recv, 0) < 10240 THEN 'Small'
            WHEN COALESCE(r.bytes_sent, 0) + COALESCE(r.bytes_recv, 0) < 102400 THEN 'Medium'
            WHEN COALESCE(r.bytes_sent, 0) + COALESCE(r.bytes_recv, 0) < 1048576 THEN 'Large'
            ELSE 'Huge'
        END AS size_band,
        -- Visitor lookup key
        CASE WHEN r.client_ip IN (
            SELECT ip FROM crawler_ips_list
        ) THEN 1 ELSE 2 END AS visitor_key
    FROM raw r
)

SELECT
    c.raw_log_id,
    c.source_file,
    COALESCE(d.date_sk, -1) AS date_sk,
    COALESCE(t.time_sk, -1) AS time_sk,
    COALESCE(p.page_sk, -1) AS page_sk,
    COALESCE(m.method_sk, -1) AS method_sk,
    COALESCE(s.status_sk, -1) AS status_sk,
    COALESCE(rf.referrer_sk, -1) AS referrer_sk,
    COALESCE(g.geolocation_sk, -1) AS geolocation_sk,
    COALESCE(ua.user_agent_sk, -1) AS user_agent_sk,
    COALESCE(v.visitor_sk, -1) AS visitor_sk,
    COALESCE(vb.visit_bucket_sk, -1) AS visit_bucket_sk,
    c.bytes_sent,
    c.bytes_received,
    c.response_time_ms,
    c.request_count,
    c.is_404,
    c.is_crawler,
    c.is_direct_traffic,
    c.size_band,
    t.time_band
FROM computed c
LEFT JOIN {{ ref('dim_date') }} d ON d.date = c.log_date
LEFT JOIN {{ ref('dim_time') }} t
    ON t.hour = EXTRACT(HOUR FROM c.log_time)::INTEGER
    AND t.minute = EXTRACT(MINUTE FROM c.log_time)::INTEGER
LEFT JOIN page_map p
    ON p.page_path = c.uri_stem
    AND p.query_string = c.uri_query
LEFT JOIN method_map m ON m.http_method = c.method_name
LEFT JOIN status_map s
    ON s.status_code = c.status_code
    AND s.sub_status = c.sub_status
    AND s.win32_status = c.win32_status
LEFT JOIN referrer_map rf ON rf.referrer_url = c.referrer_url
LEFT JOIN geo_map g ON g.client_ip = c.client_ip
LEFT JOIN ua_map ua ON ua.user_agent = c.user_agent
LEFT JOIN {{ source('w3c', 'dim_visitortype') }} v ON v.visitor_sk = c.visitor_key
LEFT JOIN ip_visit_buckets ib ON ib.client_ip = c.client_ip
LEFT JOIN {{ ref('dim_visit_buckets') }} vb ON vb.visit_bucket = ib.visit_bucket_name

ORDER BY c.raw_log_id
