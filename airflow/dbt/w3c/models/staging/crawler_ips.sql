{{ config(materialized='table', tags=['staging', 'dbt']) }}

SELECT DISTINCT client_ip AS ip
FROM {{ source('w3c', 'raw_enriched') }}
{% if target.type == 'sqlserver' %}
WHERE is_crawler = 1
{% else %}
WHERE is_crawler = TRUE
{% endif %}
