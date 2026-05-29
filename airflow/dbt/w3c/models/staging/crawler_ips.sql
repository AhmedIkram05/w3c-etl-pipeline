{{ config(materialized='table', tags=['staging', 'dbt']) }}

SELECT DISTINCT client_ip AS ip
FROM {{ source('w3c', 'raw_logs') }}
WHERE (uri_stem ILIKE '%/robots.txt' OR uri_stem ILIKE 'robots.txt')
  AND client_ip IS NOT NULL
