{{ config(
    materialized='table',
    tags=['dimension', 'dbt'],
    post_hook=[
        "CREATE INDEX IF NOT EXISTS idx_dim_referrer_domain ON {{ this }}(referrer_domain)"
    ]
) }}

WITH raw_referrers AS (
    SELECT DISTINCT
        CASE WHEN referrer IS NULL OR TRIM(referrer) IN ('', '-') THEN 'Direct' ELSE TRIM(referrer) END AS referrer_url
    FROM {{ source('w3c', 'raw_logs') }}
),

referrer_entries AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY rr.referrer_url) AS referrer_sk,
        rr.referrer_url,
        CASE
            WHEN rr.referrer_url = 'Direct' THEN 'Direct'
            WHEN rr.referrer_url ~* '^https?://([^/]+)'
                THEN LOWER(REGEXP_REPLACE(rr.referrer_url, '^https?://([^/]+).*', '\1'))
            WHEN rr.referrer_url ~* '^https?://'
                THEN LOWER(SPLIT_PART(REPLACE(REPLACE(rr.referrer_url, 'http://', ''), 'https://', ''), '/', 1))
            ELSE 'Unknown'
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
    FROM raw_referrers rr
)

SELECT * FROM referrer_entries
ORDER BY referrer_sk
