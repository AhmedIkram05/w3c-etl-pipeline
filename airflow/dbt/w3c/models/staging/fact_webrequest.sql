{{ config(
    materialized='incremental',
    unique_key='raw_log_id',
    on_schema_change='append_new_columns',
    tags=['fact', 'dbt']
) }}

WITH enriched_input AS (
    SELECT
        *,
        -- Stable unique key (raw_log_id) generated since source has no auto-increment
        -- Includes enough dimensions to disambiguate concurrent identical-looking requests:
        -- source_file, log_time, client_ip, user_agent, referrer, uri_stem, uri_query,
        -- method, status, sub_status, win32_status, time_taken
        {{ tsql_hash_md5("CONCAT(source_file, '|', log_time, '|', client_ip, '|', user_agent, '|', referrer, '|', uri_stem, '|', uri_query, '|', method, '|', status, '|', sub_status, '|', win32_status, '|', time_taken)") }} AS raw_log_id,
        {% if target.type == 'sqlserver' %}
            CASE WHEN status = 404 THEN 1 ELSE 0 END AS is_404,
            CASE WHEN referrer = '-' OR referrer IS NULL THEN 1 ELSE 0 END AS is_direct_traffic,
        {% else %}
            CASE WHEN status = 404 THEN TRUE ELSE FALSE END AS is_404,
            CASE WHEN referrer = '-' OR referrer IS NULL THEN TRUE ELSE FALSE END AS is_direct_traffic,
        {% endif %}
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
        {{ tsql_cast('ei.log_date', 'DATE') }} AS log_date,
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
        {{ tsql_cast('COALESCE(ei.bytes_sent, 0)', 'BIGINT') }} AS bytes_sent,
        {{ tsql_cast('COALESCE(ei.bytes_recv, 0)', 'BIGINT') }} AS bytes_received,
        {{ tsql_cast('COALESCE(ei.time_taken, 0)', 'INT') }} AS response_time_ms,
        ei.request_count,
        ei.is_404,
        -- is_crawler is pre-computed in raw_enriched by the Spark pipeline
        {% if target.type == 'sqlserver' %}
            COALESCE(ei.is_crawler, 0) AS is_crawler,
        {% else %}
            COALESCE(ei.is_crawler, FALSE) AS is_crawler,
        {% endif %}
        ei.is_direct_traffic,
        -- size_band is pre-computed in raw_enriched by the Spark pipeline
        COALESCE(ei.size_band, 'Zero') AS size_band,

        -- Computed enrichment (denormalized from raw_enriched)
        ei.page_category,
        ei.referrer_domain,
        ei.traffic_type,
        {% if target.type == 'sqlserver' %}
            -- Geo columns for dim_geolocation FK restoration (sqlserver only)
            ei.country,
            ei.region,
            ei.city,
            ei.latitude,
            ei.longitude,
        {% endif %}
        -- Visitor lookup key from is_crawler flag
        {% if target.type == 'sqlserver' %}
            CASE WHEN COALESCE(ei.is_crawler, 0) = 1 THEN 1 ELSE 2 END AS visitor_key
        {% else %}
            CASE WHEN COALESCE(ei.is_crawler, FALSE) THEN 1 ELSE 2 END AS visitor_key
        {% endif %}
    FROM enriched_input ei
),

{% if target.type == 'sqlserver' %}
geo_lookup AS (
    SELECT DISTINCT
        c.client_ip,
        CONVERT(NVARCHAR(64), HASHBYTES('SHA2_256',
            ISNULL(c.country, '') + '|'
            + ISNULL(c.region, '') + '|'
            + ISNULL(c.city, '') + '|'
            + ISNULL(CAST(c.latitude AS NVARCHAR), '') + '|'
            + ISNULL(CAST(c.longitude AS NVARCHAR), '')
        ), 2) AS geo_hash
    FROM computed c
    WHERE c.country IS NOT NULL
),
ua_lookup AS (
    SELECT DISTINCT
        c.user_agent,
        ua.user_agent_sk
    FROM computed c
    LEFT JOIN {{ source('w3c', 'dim_useragent') }} ua ON ua.user_agent = c.user_agent
    WHERE c.user_agent IS NOT NULL AND c.user_agent != '-'
)
{% endif %}

SELECT
    c.raw_log_id,
    c.source_file,
    d.date_sk,
    t.time_sk,
    p.page_sk,
    m.method_sk,
    s.status_sk,
    rf.referrer_sk,
    {% if target.type == 'sqlserver' %}
        COALESCE(g.geolocation_sk, -1) AS geolocation_sk,
        COALESCE(ua.user_agent_sk, -1) AS user_agent_sk,
    {% else %}
        -1 AS geolocation_sk,
        -1 AS user_agent_sk,
    {% endif %}
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
    ON t.hour = {{ tsql_cast(tsql_datepart('HOUR', tsql_cast('c.log_time', 'TIME')), 'INT') }}
    AND t.minute = {{ tsql_cast(tsql_datepart('MINUTE', tsql_cast('c.log_time', 'TIME')), 'INT') }}
INNER JOIN page_map p
    ON p.page_path = c.uri_stem
    AND p.query_string = c.uri_query
INNER JOIN method_map m ON m.http_method = c.method_name
INNER JOIN status_map s
    ON s.status_code = c.status_code
    AND s.sub_status = c.sub_status
    AND s.win32_status = c.win32_status
INNER JOIN referrer_map rf ON rf.referrer_url = c.referrer_url
INNER JOIN {{ ref('dim_visitortype') }} v ON v.visitor_sk = c.visitor_key
INNER JOIN ip_visit_buckets ib ON ib.client_ip = c.client_ip
INNER JOIN {{ ref('dim_visit_buckets') }} vb ON vb.visit_bucket = ib.visit_bucket_name
{% if target.type == 'sqlserver' %}
LEFT JOIN geo_lookup gl ON gl.client_ip = c.client_ip
LEFT JOIN {{ source('w3c', 'dim_geolocation') }} g ON g.geo_hash = gl.geo_hash
LEFT JOIN ua_lookup ua ON ua.user_agent = c.user_agent
{% endif %}
