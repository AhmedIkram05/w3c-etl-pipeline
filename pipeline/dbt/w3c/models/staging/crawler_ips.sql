{{ config(materialized='table', tags=['staging', 'dbt']) }}

SELECT DISTINCT client_ip AS ip
FROM {{ source('w3c', 'raw_enriched') }}
WHERE is_crawler = {{ tsql_true_val() }}
