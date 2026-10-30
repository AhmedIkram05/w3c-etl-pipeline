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
        -- method, status, sub_status, win32_status, time_taken.
        -- The nullable text components are COALESCEd to a '~NULL~' sentinel before
        -- hashing: CONCAT renders both NULL and '' as '', so a NULL referrer row and
        -- an empty-string referrer row used to share a key (one deduped away). No IIS
        -- log field value can be '~NULL~', so NULL and '' now hash differently on both
        -- targets. Numeric components stay bare: a NULL INT renders as '' but no real
        -- integer does, so they cannot collide.
        {{ tsql_hash_md5("CONCAT(source_file, '|', COALESCE(log_time, '~NULL~'), '|', COALESCE(client_ip, '~NULL~'), '|', COALESCE(user_agent, '~NULL~'), '|', COALESCE(referrer, '~NULL~'), '|', uri_stem, '|', uri_query, '|', method, '|', status, '|', sub_status, '|', win32_status, '|', time_taken)") }} AS raw_log_id,
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
    -- Only process records from the latest captured date onward. `>=` (not `>`)
    -- keeps same-day late arrivals: the boundary day is re-merged (idempotent
    -- on the deterministic raw_log_id) instead of being silently dropped.
    -- raw_enriched is append-only (new log files arrive daily via DLT pipeline),
    -- so this prevents MERGE from scanning/reprocessing all 150k+ existing rows.
    WHERE log_date >= (SELECT MAX(log_date) FROM {{ this }})
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
    -- Visit buckets are defined PER CALENDAR DAY: COUNT(*) is the number of
    -- requests one client IP made on a single log_date, not its lifetime
    -- request total. Per-day counts are stable once a day is loaded, so
    -- historical fact rows are never re-bucketed retroactively (the previous
    -- lifetime GROUP BY changed existing rows' buckets as history grew).
    -- The aggregate reads enriched_input itself, so on incremental runs only
    -- the watermark slice is scanned — no full-history GROUP BY.
    SELECT
        ei.client_ip,
        ei.log_date,
        CASE
            WHEN COUNT(*) = 1 THEN '1 Visit'
            WHEN COUNT(*) <= 5 THEN '2-5 Visits'
            WHEN COUNT(*) <= 10 THEN '6-10 Visits'
            WHEN COUNT(*) <= 20 THEN '11-20 Visits'
            WHEN COUNT(*) <= 50 THEN '21-50 Visits'
            ELSE '51+ Visits'
        END AS visit_bucket_name
    FROM enriched_input ei
    WHERE ei.client_ip IS NOT NULL AND ei.client_ip != '-'
    GROUP BY ei.client_ip, ei.log_date
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
    -- Exactly one geo_hash per client_ip. A single IP whose rows carry two
    -- different geo tuples would otherwise yield two rows here and fan out the
    -- fact's LEFT JOIN, duplicating raw_log_id inside one incremental MERGE
    -- ("cannot UPDATE the same row twice"). ROW_NUMBER over the geo tuple picks
    -- one deterministically. Coordinates are hashed at fixed DECIMAL(18,10)
    -- precision (not the ~6-significant-digit default FLOAT->NVARCHAR cast), so
    -- coordinates differing past the 6th digit no longer collide on geo_hash.
    SELECT client_ip, geo_hash
    FROM (
        SELECT
            c.client_ip,
            CONVERT(NVARCHAR(64), HASHBYTES('SHA2_256',
                ISNULL(c.country, '') + '|'
                + ISNULL(c.region, '') + '|'
                + ISNULL(c.city, '') + '|'
                + ISNULL(CONVERT(NVARCHAR(32), CONVERT(DECIMAL(18,10), c.latitude)), '') + '|'
                + ISNULL(CONVERT(NVARCHAR(32), CONVERT(DECIMAL(18,10), c.longitude)), '')
            ), 2) AS geo_hash,
            ROW_NUMBER() OVER (
                PARTITION BY c.client_ip
                ORDER BY c.country, c.region, c.city, c.latitude, c.longitude
            ) AS rn
        FROM computed c
        WHERE c.country IS NOT NULL
    ) g
    WHERE g.rn = 1
),
{% else %}
geo_lookup AS (
    SELECT DISTINCT
        c.client_ip,
        g.geolocation_sk
    FROM computed c
    LEFT JOIN {{ source('w3c', 'dim_geolocation') }} g ON g.ip = c.client_ip
    WHERE g.geolocation_sk IS NOT NULL
),
{% endif %}
ua_lookup AS (
    SELECT DISTINCT
        c.user_agent,
        ua.user_agent_sk
    FROM computed c
    LEFT JOIN {{ source('w3c', 'dim_useragent') }} ua ON ua.user_agent = LEFT(c.user_agent, 1000)
    WHERE c.user_agent IS NOT NULL AND c.user_agent != '-'
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
    {% if target.type == 'sqlserver' %}
        COALESCE(g.geolocation_sk, -1) AS geolocation_sk,
        COALESCE(ua.user_agent_sk, -1) AS user_agent_sk,
    {% else %}
        COALESCE(gl.geolocation_sk, -1) AS geolocation_sk,
        COALESCE(ua.user_agent_sk, -1) AS user_agent_sk,
    {% endif %}
    v.visitor_sk,
    COALESCE(vb.visit_bucket_sk, -1) AS visit_bucket_sk,
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
    t.time_band,
    d.is_weekend,
    d.day_of_week,
    t.hour AS request_hour
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
-- LEFT JOINs keep requests whose client_ip is NULL/'-' (they have no bucket
-- row); the old INNER JOIN dropped them from the fact. COALESCE maps them to
-- dim_visit_buckets' 'Unknown' (-1) member. ip_visit_buckets still excludes
-- NULL/'-' so the real bucket counts are not inflated.
LEFT JOIN ip_visit_buckets ib
    ON ib.client_ip = c.client_ip
    AND ib.log_date = c.log_date
LEFT JOIN {{ ref('dim_visit_buckets') }} vb
    ON vb.visit_bucket = COALESCE(ib.visit_bucket_name, 'Unknown')
LEFT JOIN geo_lookup gl ON gl.client_ip = c.client_ip
{% if target.type == 'sqlserver' %}
-- dim_geolocation is SCD Type 2 (multiple versions per geo_hash): join to the
-- current version only, otherwise historical rows fan out every fact row.
LEFT JOIN {{ source('w3c', 'dim_geolocation') }} g ON g.geo_hash = gl.geo_hash AND g.is_current = 1
{% endif %}
LEFT JOIN ua_lookup ua ON ua.user_agent = c.user_agent
