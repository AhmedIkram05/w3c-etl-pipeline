{{ config(severity='error') }}

-- Singular test: fact_webrequest dedup key disambiguates rows that differ on ANY of
-- the 12 fields hashed into raw_log_id.
--
-- Purpose: The fact model's MD5 dedup key collapses raw_enriched rows that share the
-- same hash. If a single hash bucket contains 2+ distinct values for ANY of the
-- twelve fields the key is supposed to disambiguate, the dedup is silently dropping
-- legitimate events. This includes the NULL-vs-empty-string case: CONCAT renders
-- both as '', so a NULL referrer row and an empty-string referrer row used to share
-- a key and one was deduped away. The model COALESCEs the nullable text fields to
-- the '~NULL~' sentinel before hashing, and this test counts NULL-aware distinct
-- values with the same sentinel — plain COUNT(DISTINCT) ignores NULLs and would
-- miss exactly that collision.
--
-- This test runs directly against {{ source('w3c', 'raw_enriched') }} (the same input
-- the fact model hashes) so it tests the contract of the new key without depending
-- on the fact table itself being correct.
--
-- If the key is well-formed, every hash bucket will have COUNT(DISTINCT ...) = 1
-- for each of the twelve fields, and the HAVING clause returns no rows.

WITH hashed AS (
    SELECT
        *,
        {{ tsql_hash_md5("CONCAT(source_file, '|', COALESCE(log_time, '~NULL~'), '|', COALESCE(client_ip, '~NULL~'), '|', COALESCE(user_agent, '~NULL~'), '|', COALESCE(referrer, '~NULL~'), '|', uri_stem, '|', uri_query, '|', method, '|', status, '|', sub_status, '|', win32_status, '|', time_taken)") }} AS raw_log_id
    FROM {{ source('w3c', 'raw_enriched') }}
),

rendered AS (
    SELECT
        raw_log_id,
        COALESCE(source_file, '~NULL~') AS f_source_file,
        COALESCE(log_time, '~NULL~') AS f_log_time,
        COALESCE(client_ip, '~NULL~') AS f_client_ip,
        COALESCE(user_agent, '~NULL~') AS f_user_agent,
        COALESCE(referrer, '~NULL~') AS f_referrer,
        COALESCE(uri_stem, '~NULL~') AS f_uri_stem,
        COALESCE(uri_query, '~NULL~') AS f_uri_query,
        COALESCE(method, '~NULL~') AS f_method,
        COALESCE({{ tsql_cast('status', 'VARCHAR') }}, '~NULL~') AS f_status,
        COALESCE({{ tsql_cast('sub_status', 'VARCHAR') }}, '~NULL~') AS f_sub_status,
        COALESCE({{ tsql_cast('win32_status', 'VARCHAR') }}, '~NULL~') AS f_win32_status,
        COALESCE({{ tsql_cast('time_taken', 'VARCHAR') }}, '~NULL~') AS f_time_taken
    FROM hashed
)

SELECT
    raw_log_id,
    COUNT(DISTINCT f_source_file)  AS distinct_source_file,
    COUNT(DISTINCT f_log_time)     AS distinct_log_time,
    COUNT(DISTINCT f_client_ip)    AS distinct_client_ip,
    COUNT(DISTINCT f_user_agent)   AS distinct_user_agent,
    COUNT(DISTINCT f_referrer)     AS distinct_referrer,
    COUNT(DISTINCT f_uri_stem)     AS distinct_uri_stem,
    COUNT(DISTINCT f_uri_query)    AS distinct_uri_query,
    COUNT(DISTINCT f_method)       AS distinct_method,
    COUNT(DISTINCT f_status)       AS distinct_status,
    COUNT(DISTINCT f_sub_status)   AS distinct_sub_status,
    COUNT(DISTINCT f_win32_status) AS distinct_win32_status,
    COUNT(DISTINCT f_time_taken)   AS distinct_time_taken
FROM rendered
GROUP BY raw_log_id
HAVING COUNT(DISTINCT f_source_file) > 1
    OR COUNT(DISTINCT f_log_time) > 1
    OR COUNT(DISTINCT f_client_ip) > 1
    OR COUNT(DISTINCT f_user_agent) > 1
    OR COUNT(DISTINCT f_referrer) > 1
    OR COUNT(DISTINCT f_uri_stem) > 1
    OR COUNT(DISTINCT f_uri_query) > 1
    OR COUNT(DISTINCT f_method) > 1
    OR COUNT(DISTINCT f_status) > 1
    OR COUNT(DISTINCT f_sub_status) > 1
    OR COUNT(DISTINCT f_win32_status) > 1
    OR COUNT(DISTINCT f_time_taken) > 1
