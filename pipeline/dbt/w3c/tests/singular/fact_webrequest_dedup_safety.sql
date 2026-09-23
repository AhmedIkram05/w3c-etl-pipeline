{{ config(severity='error') }}

-- Singular test: fact_webrequest dedup key disambiguates rows that differ on uri_query / method / win32_status
--
-- Purpose: The fact model's MD5 dedup key collapses raw_enriched rows that share the
-- same hash. If a single hash bucket contains 2+ distinct values for ANY of the
-- three fields the key is supposed to disambiguate (uri_query, method, win32_status),
-- the dedup is silently dropping legitimate events.
--
-- This test runs directly against {{ source('w3c', 'raw_enriched') }} (the same input
-- the fact model hashes) so it tests the contract of the new key without depending
-- on the fact table itself being correct.
--
-- If the key is well-formed, every hash bucket will have COUNT(DISTINCT ...) = 1
-- for each of the three disambiguating fields, and the HAVING clause returns no rows.

WITH key_inputs AS (
    SELECT
        source_file,
        log_time,
        client_ip,
        user_agent,
        referrer,
        uri_stem,
        uri_query,
        method,
        status,
        sub_status,
        win32_status,
        time_taken
    FROM {{ source('w3c', 'raw_enriched') }}
),

hashed AS (
    SELECT
        *,
        {{ tsql_hash_md5("CONCAT(source_file, '|', log_time, '|', client_ip, '|', user_agent, '|', referrer, '|', uri_stem, '|', uri_query, '|', method, '|', status, '|', sub_status, '|', win32_status, '|', time_taken)") }} AS raw_log_id
    FROM key_inputs
)

SELECT
    raw_log_id,
    COUNT(DISTINCT uri_query)    AS distinct_uri_query,
    COUNT(DISTINCT method)       AS distinct_method,
    COUNT(DISTINCT win32_status) AS distinct_win32_status
FROM hashed
GROUP BY raw_log_id
HAVING COUNT(DISTINCT uri_query) > 1
    OR COUNT(DISTINCT method) > 1
    OR COUNT(DISTINCT win32_status) > 1
