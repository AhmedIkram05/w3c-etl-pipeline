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
    SELECT
        client_ip,
        COUNT(*) AS visit_count
    FROM {{ source('w3c', 'raw_logs') }}
    WHERE client_ip IS NOT NULL AND client_ip != '-'
    GROUP BY client_ip
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
    COUNT(client_ip) AS ip_count
FROM ip_buckets
GROUP BY visit_bucket_sk, visit_bucket, visit_bucket_order
ORDER BY visit_bucket_order
