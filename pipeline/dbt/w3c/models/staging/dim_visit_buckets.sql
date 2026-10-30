{{ config(
    materialized='table',
    tags=['dimension', 'dbt']
) }}

WITH bucket_definitions AS (
    SELECT '1 Visit' AS visit_bucket, 1 AS visit_bucket_order, 1 AS min_visits, 1 AS max_visits
    UNION ALL
    SELECT '2-5 Visits', 2, 2, 5
    UNION ALL
    SELECT '6-10 Visits', 3, 6, 10
    UNION ALL
    SELECT '11-20 Visits', 4, 11, 20
    UNION ALL
    SELECT '21-50 Visits', 5, 21, 50
    UNION ALL
    SELECT '51+ Visits', 6, 51, NULL
),

visit_counts AS (
    -- One row per (client_ip, log_date): COUNT(*) is the IP's request count
    -- for that single day. Buckets were previously computed over LIFETIME
    -- request counts while labeled "Visits", which mis-bucketed IPs and let
    -- every run re-bucket historical fact rows retroactively.
    SELECT
        client_ip,
        log_date,
        COUNT(*) AS visit_count
    FROM {{ source('w3c', 'raw_enriched') }}
    WHERE client_ip IS NOT NULL AND client_ip != '-'
    GROUP BY client_ip, log_date
),

ip_buckets AS (
    SELECT
        vc.client_ip,
        vc.visit_count,
        bd.visit_bucket,
        bd.visit_bucket_order
    FROM visit_counts vc
    JOIN bucket_definitions bd
        ON vc.visit_count >= bd.min_visits
        AND (bd.max_visits IS NULL OR vc.visit_count <= bd.max_visits)
)

SELECT
    visit_bucket_order AS visit_bucket_sk,
    visit_bucket,
    visit_bucket_order,
    -- user_count counts (client_ip, log_date) rows — per-day visit sessions
    -- in this bucket — because with per-day bucketing one IP can appear in
    -- different buckets on different days.
    COUNT(*) AS user_count
FROM ip_buckets
GROUP BY visit_bucket_order, visit_bucket

UNION ALL

-- -1 sentinel for requests without a usable client_ip (NULL/'-'): the fact
-- maps those here so they survive instead of being dropped by its bucket join.
SELECT -1 AS visit_bucket_sk, 'Unknown' AS visit_bucket, -1 AS visit_bucket_order, 0 AS user_count
