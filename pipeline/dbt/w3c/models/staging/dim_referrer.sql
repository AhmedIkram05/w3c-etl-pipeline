{{ config(
    materialized='table',
    tags=['dimension', 'dbt']
) }}

WITH raw_referrers AS (
    SELECT DISTINCT
        CASE WHEN referrer IS NULL OR TRIM(referrer) IN ('', '-') THEN 'Direct' ELSE TRIM(referrer) END AS referrer_url
    FROM {{ source('w3c', 'raw_enriched') }}
),

referrer_domains AS (
    SELECT
        rr.referrer_url,
        {% if target.type == 'sqlserver' %}
            CASE
                WHEN rr.referrer_url = 'Direct' THEN 'Direct'
                WHEN rr.referrer_url = 'Unknown' THEN 'Unknown'
                WHEN rr.referrer_url LIKE 'http://%' OR rr.referrer_url LIKE 'https://%'
                    THEN {{ tsql_extract_domain('rr.referrer_url') }}
                -- Scheme-less referrers are real traffic: prepend a scheme so the
                -- domain is still extracted (canonical utils/transformations
                -- behavior), instead of lumping them in as 'Unknown'.
                ELSE {{ tsql_extract_domain("'http://' + rr.referrer_url") }}
            END AS referrer_domain,
        {% else %}
            CASE
                WHEN rr.referrer_url = 'Direct' THEN 'Direct'
                WHEN rr.referrer_url = 'Unknown' THEN 'Unknown'
                WHEN rr.referrer_url ~* '^https?://([^/]+)'
                    THEN LOWER(REGEXP_REPLACE(rr.referrer_url, '^https?://([^/]+).*', '\1'))
                WHEN rr.referrer_url ~* '^https?://'
                    THEN LOWER(SPLIT_PART(REPLACE(REPLACE(rr.referrer_url, 'http://', ''), 'https://', ''), '/', 1))
                -- Scheme-less referrers are real traffic: prepend a scheme so the
                -- domain is still extracted (canonical utils/transformations
                -- behavior), instead of lumping them in as 'Unknown'.
                ELSE LOWER(REGEXP_REPLACE('http://' || rr.referrer_url, '^https?://([^/]+).*', '\1'))
            END AS referrer_domain
        {% endif %}
    FROM raw_referrers rr
),

referrer_entries AS (
    SELECT
        -- Stable SK: md5 of the natural key (same rationale as dim_page).
        {{ tsql_hash_md5("CONCAT(rr.referrer_url, '|')") }} AS referrer_sk,
        rr.referrer_url,
        rr.referrer_domain,
        -- traffic_source matches whole dot-delimited labels of the extracted
        -- domain: the domain is wrapped in dots so '%.google.%' demands a
        -- label boundary on both sides — 'notgoogle.com' no longer counts as
        -- a Google referrer. The output vocabulary is unchanged.
        CASE
            WHEN rr.referrer_url = 'Direct' THEN 'Direct'
            WHEN rr.referrer_url = 'Unknown' THEN 'Unknown'
        {% if target.type == 'sqlserver' %}
            WHEN ('.' + rr.referrer_domain + '.') LIKE '%.google.%' THEN 'Search Engine'
            WHEN ('.' + rr.referrer_domain + '.') LIKE '%.bing.%' THEN 'Search Engine'
            WHEN ('.' + rr.referrer_domain + '.') LIKE '%.yahoo.%' THEN 'Search Engine'
            WHEN ('.' + rr.referrer_domain + '.') LIKE '%.facebook.%' THEN 'Social Media'
            WHEN ('.' + rr.referrer_domain + '.') LIKE '%.twitter.%' THEN 'Social Media'
            WHEN ('.' + rr.referrer_domain + '.') LIKE '%.linkedin.%' THEN 'Social Media'
            WHEN ('.' + rr.referrer_domain + '.') LIKE '%.w3c.org.%' THEN 'Internal (W3C)'
        {% else %}
            WHEN ('.' || rr.referrer_domain || '.') LIKE '%.google.%' THEN 'Search Engine'
            WHEN ('.' || rr.referrer_domain || '.') LIKE '%.bing.%' THEN 'Search Engine'
            WHEN ('.' || rr.referrer_domain || '.') LIKE '%.yahoo.%' THEN 'Search Engine'
            WHEN ('.' || rr.referrer_domain || '.') LIKE '%.facebook.%' THEN 'Social Media'
            WHEN ('.' || rr.referrer_domain || '.') LIKE '%.twitter.%' THEN 'Social Media'
            WHEN ('.' || rr.referrer_domain || '.') LIKE '%.linkedin.%' THEN 'Social Media'
            WHEN ('.' || rr.referrer_domain || '.') LIKE '%.w3c.org.%' THEN 'Internal (W3C)'
        {% endif %}
            ELSE 'Referral'
        END AS traffic_source
    FROM referrer_domains rr
)

SELECT * FROM referrer_entries
