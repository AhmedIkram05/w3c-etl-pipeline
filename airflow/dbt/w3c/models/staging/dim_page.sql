{{ config(
    materialized='table',
    tags=['dimension', 'dbt'],
    post_hook=[
        "{{ tsql_create_index_if_not_exists(this.identifier, 'idx_dim_page_path', 'page_path') }}",
        "{{ tsql_create_index_if_not_exists(this.identifier, 'idx_dim_page_category', 'page_category') }}"
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
        -- directory extraction
        CASE
            WHEN rp.uri_stem LIKE '%/robots.txt' THEN '/robots.txt'
            {% if target.type == 'sqlserver' %}
                WHEN (LEN(rp.uri_stem) - LEN(REPLACE(rp.uri_stem, '/', ''))) >= 2
                    THEN REVERSE({{ tsql_split_part('REVERSE(' ~ tsql_split_part('rp.uri_stem', '?', 1) ~ ')', '/', 2) }})
                WHEN rp.uri_stem LIKE '/%' AND CHARINDEX('/', rp.uri_stem, 2) = 0 THEN '/'
            {% else %}
                WHEN rp.uri_stem ~ '^.*/[^/]+/[^/]+$'
                    THEN REVERSE(SPLIT_PART(REVERSE(SPLIT_PART(rp.uri_stem, '?', 1)), '/', 2))
                WHEN rp.uri_stem ~ '^/[^/]+$' THEN '/'
            {% endif %}
            ELSE '/'
        END AS directory,
        -- file_name extraction
        CASE
            WHEN rp.uri_stem LIKE '%/robots.txt' THEN 'robots.txt'
            {% if target.type == 'sqlserver' %}
                WHEN rp.uri_stem LIKE '%/%'
                    THEN {{ tsql_split_part('REVERSE(' ~ tsql_split_part('REVERSE(rp.uri_stem)', '/', 1) ~ ')', '?', 1) }}
            {% else %}
                WHEN rp.uri_stem ~ '^.*/([^/]+)$'
                    THEN SPLIT_PART(REVERSE(SPLIT_PART(REVERSE(rp.uri_stem), '/', 1)), '?', 1)
            {% endif %}
            ELSE rp.uri_stem
        END AS file_name,
        -- file_extension extraction
        CASE
            {% if target.type == 'sqlserver' %}
                WHEN LOWER(rp.uri_stem) LIKE '%.aspx' OR LOWER(rp.uri_stem) LIKE '%.asp' THEN 'aspx'
                WHEN LOWER(rp.uri_stem) LIKE '%.html' OR LOWER(rp.uri_stem) LIKE '%.htm' OR LOWER(rp.uri_stem) LIKE '%.shtml' THEN 'html'
                WHEN LOWER(rp.uri_stem) LIKE '%.jpg' OR LOWER(rp.uri_stem) LIKE '%.jpeg' OR LOWER(rp.uri_stem) LIKE '%.png' OR LOWER(rp.uri_stem) LIKE '%.gif' OR LOWER(rp.uri_stem) LIKE '%.bmp' OR LOWER(rp.uri_stem) LIKE '%.webp' THEN 'image'
                WHEN LOWER(rp.uri_stem) LIKE '%.ico' THEN 'ico'
                WHEN LOWER(rp.uri_stem) LIKE '%.css' THEN 'css'
                WHEN LOWER(rp.uri_stem) LIKE '%.js' THEN 'js'
                WHEN LOWER(rp.uri_stem) LIKE '%.txt' OR LOWER(rp.uri_stem) LIKE '%.xml' THEN 'text'
                WHEN LOWER(rp.uri_stem) LIKE '%.pdf' THEN 'pdf'
                WHEN rp.uri_stem = '/' OR rp.uri_stem = '' THEN 'root'
                WHEN rp.uri_stem LIKE '%.%' THEN
                    LOWER(REVERSE({{ tsql_split_part('REVERSE(' ~ tsql_split_part('rp.uri_stem', '?', 1) ~ ')', '.', 1) }}))
            {% else %}
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
            {% endif %}
            ELSE 'no_extension'
        END AS file_extension,
        -- page_category extraction
        CASE
            {% if target.type == 'sqlserver' %}
                WHEN LOWER(rp.uri_stem) LIKE '%.aspx' OR LOWER(rp.uri_stem) LIKE '%.asp' THEN 'Dynamic Page'
                WHEN LOWER(rp.uri_stem) LIKE '%.html' OR LOWER(rp.uri_stem) LIKE '%.htm' OR LOWER(rp.uri_stem) LIKE '%.shtml' THEN 'Static Page'
                WHEN LOWER(rp.uri_stem) LIKE '%.jpg' OR LOWER(rp.uri_stem) LIKE '%.jpeg' OR LOWER(rp.uri_stem) LIKE '%.png' OR LOWER(rp.uri_stem) LIKE '%.gif' OR LOWER(rp.uri_stem) LIKE '%.bmp' OR LOWER(rp.uri_stem) LIKE '%.webp' THEN 'Image'
                WHEN LOWER(rp.uri_stem) LIKE '%.ico' THEN 'Icon'
                WHEN LOWER(rp.uri_stem) LIKE '%.css' THEN 'Stylesheet'
                WHEN LOWER(rp.uri_stem) LIKE '%.js' THEN 'Script'
                WHEN LOWER(rp.uri_stem) LIKE '%.txt' OR LOWER(rp.uri_stem) LIKE '%.xml' THEN 'Text File'
                WHEN LOWER(rp.uri_stem) LIKE '%.pdf' THEN 'Document'
                WHEN rp.uri_stem LIKE '%.' OR rp.uri_stem = '/' OR rp.uri_stem = '' THEN 'Directory'
            {% else %}
                WHEN rp.uri_stem ~* '\.(aspx|asp)$' THEN 'Dynamic Page'
                WHEN rp.uri_stem ~* '\.(html?|shtml)$' THEN 'Static Page'
                WHEN rp.uri_stem ~* '\.(jpg|jpeg|png|gif|bmp|webp)$' THEN 'Image'
                WHEN rp.uri_stem ~* '\.ico$' THEN 'Icon'
                WHEN rp.uri_stem ~* '\.css$' THEN 'Stylesheet'
                WHEN rp.uri_stem ~* '\.js$' THEN 'Script'
                WHEN rp.uri_stem ~* '\.(txt|xml)$' THEN 'Text File'
                WHEN rp.uri_stem ~* '\.pdf$' THEN 'Document'
                WHEN rp.uri_stem ~ '\.$' OR rp.uri_stem = '/' OR rp.uri_stem = '' THEN 'Directory'
            {% endif %}
            ELSE 'Other'
        END AS page_category
    FROM raw_pages rp
)

SELECT * FROM page_entries
