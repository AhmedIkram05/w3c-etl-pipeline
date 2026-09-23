{{ config(
    materialized='table',
    tags=['dimension', 'dbt']
) }}

WITH raw_methods AS (
    SELECT DISTINCT
        COALESCE(NULLIF(TRIM(UPPER(method)), ''), 'Unknown') AS method_name
    FROM {{ source('w3c', 'raw_enriched') }}
    WHERE method IS NOT NULL
),

method_entries AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY rm.method_name) AS method_sk,
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
