{{ config(
    materialized='table',
    tags=['dimension', 'dbt']
) }}

WITH raw_pages AS (
    -- NULL uri_stem is kept so the COALESCE emits the 'Unknown' member the
    -- fact maps it to; excluding it left that member missing and the fact's
    -- INNER JOIN silently dropped the rows.
    SELECT DISTINCT
        COALESCE(NULLIF(TRIM(uri_stem), ''), 'Unknown') AS uri_stem,
        COALESCE(NULLIF(TRIM(uri_query), ''), '-') AS uri_query
    FROM {{ source('w3c', 'raw_enriched') }}
),

page_paths AS (
    SELECT
        uri_stem,
        uri_query,
        -- Path part only (query string is stored separately in uri_query, but
        -- some stems embed one); both dialects split on the first '?'.
        {% if target.type == 'sqlserver' %}
            {{ tsql_split_part('uri_stem', '?', 1) }} AS path
        {% else %}
            SPLIT_PART(uri_stem, '?', 1) AS path
        {% endif %}
    FROM raw_pages
),

page_entries AS (
    SELECT
        -- Stable SK: md5 of the natural key. This dim is rebuilt every run while
        -- fact_webrequest is incremental; a ROW_NUMBER sort position re-pointed
        -- every historical fact FK whenever a new member sorted earlier.
        {{ tsql_hash_md5("CONCAT(rp.uri_stem, '|', rp.uri_query)") }} AS page_sk,
        rp.uri_stem AS page_path,
        rp.uri_query AS query_string,
        -- directory extraction: everything before the last '/', or '/' when the
        -- stem has no '/' or is slash-rooted. Both dialects use the same rule
        -- (last-slash distance via REVERSE) so they no longer disagree on
        -- trailing-slash / multi-segment stems.
        CASE
            WHEN rp.uri_stem LIKE '%/robots.txt' THEN '/robots.txt'
            {% if target.type == 'sqlserver' %}
                WHEN CHARINDEX('/', REVERSE(rp.path)) = 0
                    OR CHARINDEX('/', REVERSE(rp.path)) = LEN(rp.path) THEN '/'
                ELSE LEFT(rp.path, LEN(rp.path) - CHARINDEX('/', REVERSE(rp.path)))
            {% else %}
                WHEN STRPOS(REVERSE(rp.path), '/') = 0
                    OR STRPOS(REVERSE(rp.path), '/') = LENGTH(rp.path) THEN '/'
                ELSE LEFT(rp.path, LENGTH(rp.path) - STRPOS(REVERSE(rp.path), '/'))
            {% endif %}
        END AS directory,
        -- file_name extraction: the segment after the last '/', or the whole
        -- path when there is no '/'.
        CASE
            WHEN rp.uri_stem LIKE '%/robots.txt' THEN 'robots.txt'
            {% if target.type == 'sqlserver' %}
                WHEN CHARINDEX('/', REVERSE(rp.path)) = 0 THEN rp.path
                ELSE RIGHT(rp.path, CHARINDEX('/', REVERSE(rp.path)) - 1)
            {% else %}
                WHEN STRPOS(REVERSE(rp.path), '/') = 0 THEN rp.path
                ELSE RIGHT(rp.path, STRPOS(REVERSE(rp.path), '/') - 1)
            {% endif %}
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
    FROM page_paths rp
)

SELECT * FROM page_entries
