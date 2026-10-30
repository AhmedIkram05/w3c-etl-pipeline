{{ config(
    materialized='table',
    tags=['dimension', 'dbt']
) }}

WITH raw_methods AS (
    -- NULL methods are kept so the COALESCE below emits the 'Unknown' member
    -- the fact maps them to; excluding them left that member missing and the
    -- fact's INNER JOIN silently dropped the rows.
    SELECT DISTINCT
        COALESCE(NULLIF(TRIM(UPPER(method)), ''), 'Unknown') AS method_name
    FROM {{ source('w3c', 'raw_enriched') }}
),

method_entries AS (
    SELECT
        -- Stable SK: md5 of the natural key (same rationale as dim_page).
        {{ tsql_hash_md5("CONCAT(rm.method_name, '|')") }} AS method_sk,
        rm.method_name AS http_method,
        CASE
            WHEN rm.method_name = 'GET' THEN 'Retrieve a resource'
            WHEN rm.method_name = 'POST' THEN 'Submit data to the server'
            WHEN rm.method_name = 'PUT' THEN 'Upload a resource'
            WHEN rm.method_name = 'DELETE' THEN 'Delete a resource'
            WHEN rm.method_name = 'HEAD' THEN 'Retrieve headers only'
            WHEN rm.method_name = 'OPTIONS' THEN 'Describe communication options'
            WHEN rm.method_name = 'PATCH' THEN 'Partial resource update'
            ELSE 'Other HTTP Method'
        END AS description,
        CASE
            WHEN rm.method_name IN ('GET', 'HEAD', 'OPTIONS') THEN 'Safe'
            WHEN rm.method_name IN ('POST', 'PUT', 'PATCH', 'DELETE') THEN 'Unsafe'
            ELSE 'Unknown'
        END AS is_safe
    FROM raw_methods rm
)

SELECT * FROM method_entries
