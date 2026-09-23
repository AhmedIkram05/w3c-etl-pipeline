{{ config(materialized='table', tags=['staging', 'dbt']) }}

SELECT -1 AS visitor_sk, 'Ukn' AS crawler_flag, 'Unknown' AS visitor_type
UNION ALL
SELECT 1, 'Yes', 'Crawler'
UNION ALL
SELECT 2, 'No', 'Human'
