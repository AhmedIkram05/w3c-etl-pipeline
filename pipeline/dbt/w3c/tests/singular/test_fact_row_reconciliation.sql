{{ config(severity='error') }}

-- Singular test: reconcile the fact row count against the loaded raw_enriched slice.
--
-- test_row_count_consistency.sql only catches an empty fact, duplicate
-- raw_log_id values and a missing date. It cannot see a dimensional INNER JOIN
-- that silently stops matching (e.g. a dimension missing a member) — that drops
-- fact rows while every other check stays green.
--
-- fact_webrequest emits exactly one row per distinct raw_log_id (it dedupes with
-- ROW_NUMBER) and only ever drops rows through its INNER JOINs to dim_date /
-- dim_time / dim_page / dim_method / dim_status / dim_referrer /
-- dim_visitortype. With every dimension complete, the fact row count must equal
-- the number of DISTINCT raw_log_id recomputed over the same loaded slice of
-- raw_enriched; any shortfall means a join dropped rows.
--
-- The slice is bounded by the fact's own [MIN(log_date), MAX(log_date)] window
-- so append-only raw_enriched rows outside the loaded window (not yet processed,
-- or back-dated past the model's watermark) do not create false failures.
--
-- raw_log_id is recomputed with the exact expression from fact_webrequest.sql
-- (same macros and '~NULL~' sentinels); the test fails loudly if the model's key
-- definition and this copy drift apart.

WITH loaded_slice AS (
    SELECT
        {{ tsql_hash_md5("CONCAT(source_file, '|', COALESCE(log_time, '~NULL~'), '|', COALESCE(client_ip, '~NULL~'), '|', COALESCE(user_agent, '~NULL~'), '|', COALESCE(referrer, '~NULL~'), '|', uri_stem, '|', uri_query, '|', method, '|', status, '|', sub_status, '|', win32_status, '|', time_taken)") }} AS raw_log_id
    FROM {{ source('w3c', 'raw_enriched') }}
    WHERE {{ tsql_cast('log_date', 'DATE') }} BETWEEN
        (SELECT MIN(log_date) FROM {{ ref('fact_webrequest') }})
        AND (SELECT MAX(log_date) FROM {{ ref('fact_webrequest') }})
),

expected AS (
    SELECT COUNT(DISTINCT raw_log_id) AS expected_rows FROM loaded_slice
),

actual AS (
    SELECT COUNT(*) AS fact_rows FROM {{ ref('fact_webrequest') }}
)

SELECT
    expected_rows,
    fact_rows
FROM expected
CROSS JOIN actual
WHERE fact_rows < expected_rows
