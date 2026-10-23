{{ config(severity='error') }}

-- Singular test: dimension surrogate keys must be deterministic hashes of the natural key.
--
-- The four dimensions are rebuilt every run (materialized='table') while
-- fact_webrequest is incremental. An SK derived from a position in the current
-- sort (ROW_NUMBER) silently re-points every existing fact FK whenever a new
-- member sorts earlier. This test asserts each SK equals the md5 of its
-- natural-key columns -- the exact expression the dim models use -- so any
-- drift between a dim rebuild and the fact's stored FKs fails loudly.
--
-- This asserts value determinism (SK == hash of natural key), not cross-run
-- stability; the expression equality makes any nondeterministic change in the
-- dim models fail here.

WITH sk_mismatches AS (
    SELECT 'dim_page' AS dim_name, page_sk AS sk
    FROM {{ ref('dim_page') }}
    WHERE page_sk != {{ tsql_hash_md5("CONCAT(page_path, '|', query_string)") }}
    UNION ALL
    SELECT 'dim_method' AS dim_name, method_sk AS sk
    FROM {{ ref('dim_method') }}
    WHERE method_sk != {{ tsql_hash_md5("CONCAT(http_method, '|')") }}
    UNION ALL
    SELECT 'dim_status' AS dim_name, status_sk AS sk
    FROM {{ ref('dim_status') }}
    WHERE status_sk != {{ tsql_hash_md5("CONCAT(status_code, '|', sub_status, '|', win32_status)") }}
    UNION ALL
    SELECT 'dim_referrer' AS dim_name, referrer_sk AS sk
    FROM {{ ref('dim_referrer') }}
    WHERE referrer_sk != {{ tsql_hash_md5("CONCAT(referrer_url, '|')") }}
)

SELECT * FROM sk_mismatches
