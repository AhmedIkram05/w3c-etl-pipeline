{{ config(
    materialized='table',
    tags=['dimension', 'dbt'],
    post_hook=[
        "CREATE INDEX IF NOT EXISTS idx_dim_page_path ON {{ this }}(page_path)",
        "CREATE INDEX IF NOT EXISTS idx_dim_page_category ON {{ this }}(page_category)"
    ]
) }}

WITH raw_pages AS (
    SELECT DISTINCT
        COALESCE(NULLIF(TRIM(uri_stem), ''), 'Unknown') AS uri_stem,
        COALESCE(NULLIF(TRIM(uri_query), ''), '-') AS uri_query
    FROM {{ source('w3c', 'raw_enriched') }}
    WHERE uri_stem IS NOT NULL
),

page_entries AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY rp.uri_stem, rp.uri_query) AS page_sk,
        rp.uri_stem AS page_path,
        rp.uri_query AS query_string,
        CASE
            WHEN rp.uri_stem LIKE '%/robots.txt' THEN '/robots.txt'
            WHEN rp.uri_stem ~ '^.*/[^/]+/[^/]+$'
                THEN REVERSE(SPLIT_PART(REVERSE(SPLIT_PART(rp.uri_stem, '?', 1)), '/', 2))
            WHEN rp.uri_stem ~ '^/[^/]+$' THEN '/'
            ELSE '/'
        END AS directory,
        CASE
            WHEN rp.uri_stem LIKE '%/robots.txt' THEN 'robots.txt'
            WHEN rp.uri_stem ~ '^.*/([^/]+)$'
                THEN SPLIT_PART(REVERSE(SPLIT_PART(REVERSE(rp.uri_stem), '/', 1)), '?', 1)
            ELSE rp.uri_stem
        END AS file_name,
        CASE
            WHEN rp.uri_stem ~* '\.(aspx|asp)$' THEN 'aspx'
            WHEN rp.uri_stem ~* '\.(html?|shtml)$' THEN 'html'
            WHEN rp.uri_stem ~* '\.(jpg|jpeg|png|gif|bmp|webp)$' THEN 'image'
            WHEN rp.uri_stem ~* '\.ico$' THEN 'ico'
            WHEN rp.uri_stem ~* '\.css$' THEN 'css'
            WHEN rp.uri_stem ~* '\.js$' THEN 'js'
            WHEN rp.uri_stem ~* '\.(txt|xml)$' THEN 'text'
            WHEN rp.uri_stem ~* '\.pdf$' THEN 'pdf'
            WHEN rp.uri_stem = '/' OR rp.uri_stem = '' THEN 'root'
            WHEN rp.uri_stem ~ '\.' THEN
                LOWER(REVERSE(SPLIT_PART(REVERSE(SPLIT_PART(rp.uri_stem, '?', 1)), '.', 1)))
            ELSE 'no_extension'
        END AS file_extension,
        CASE
            WHEN rp.uri_stem ~* '\.(aspx|asp)$' THEN 'Dynamic Page'
            WHEN rp.uri_stem ~* '\.(html?|shtml)$' THEN 'Static Page'
            WHEN rp.uri_stem ~* '\.(jpg|jpeg|png|gif|bmp|webp)$' THEN 'Image'
            WHEN rp.uri_stem ~* '\.ico$' THEN 'Icon'
            WHEN rp.uri_stem ~* '\.css$' THEN 'Stylesheet'
            WHEN rp.uri_stem ~* '\.js$' THEN 'Script'
            WHEN rp.uri_stem ~* '\.(txt|xml)$' THEN 'Text File'
            WHEN rp.uri_stem ~* '\.pdf$' THEN 'Document'
            WHEN rp.uri_stem ~ '\.$' OR rp.uri_stem = '/' OR rp.uri_stem = '' THEN 'Directory'
            ELSE 'Other'
        END AS page_category
    FROM raw_pages rp
)

SELECT * FROM page_entries
