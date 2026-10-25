{{ config(
    materialized='table',
    tags=['dimension', 'dbt']
) }}

WITH raw_referrers AS (
    SELECT DISTINCT
        CASE WHEN referrer IS NULL OR TRIM(referrer) IN ('', '-') THEN 'Direct' ELSE TRIM(referrer) END AS referrer_url
    FROM {{ source('w3c', 'raw_enriched') }}
),

referrer_entries AS (
    SELECT
        -- Stable SK: md5 of the natural key (same rationale as dim_page).
        {{ tsql_hash_md5("CONCAT(rr.referrer_url, '|')") }} AS referrer_sk,
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
            CASE
                WHEN rr.referrer_url = 'Direct' THEN 'Direct'
                WHEN LOWER(rr.referrer_url) LIKE '%google.%' THEN 'Search Engine'
                WHEN LOWER(rr.referrer_url) LIKE '%bing.%' THEN 'Search Engine'
                WHEN LOWER(rr.referrer_url) LIKE '%yahoo.%' THEN 'Search Engine'
                WHEN LOWER(rr.referrer_url) LIKE '%facebook.%' THEN 'Social Media'
                WHEN LOWER(rr.referrer_url) LIKE '%twitter.%' THEN 'Social Media'
                WHEN LOWER(rr.referrer_url) LIKE '%linkedin.%' THEN 'Social Media'
                WHEN LOWER(rr.referrer_url) LIKE '%w3c.org%' THEN 'Internal (W3C)'
                ELSE 'Referral'
            END AS traffic_source
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
            END AS referrer_domain,
            CASE
                WHEN rr.referrer_url = 'Direct' THEN 'Direct'
                WHEN rr.referrer_url ~* 'google\.' THEN 'Search Engine'
                WHEN rr.referrer_url ~* 'bing\.' THEN 'Search Engine'
                WHEN rr.referrer_url ~* 'yahoo\.' THEN 'Search Engine'
                WHEN rr.referrer_url ~* 'facebook\.' THEN 'Social Media'
                WHEN rr.referrer_url ~* 'twitter\.' THEN 'Social Media'
                WHEN rr.referrer_url ~* 'linkedin\.' THEN 'Social Media'
                WHEN rr.referrer_url ~* 'w3c\.org' THEN 'Internal (W3C)'
                ELSE 'Referral'
            END AS traffic_source
        {% endif %}
    FROM raw_referrers rr
)

SELECT * FROM referrer_entries
