{{ config(
    materialized='table',
    tags=['dimension', 'dbt']
) }}

WITH time_entries AS (
    SELECT
        (h.hour * 100 + m.minute) AS time_sk,
        h.hour,
        m.minute,
        CASE WHEN h.hour < 12 THEN 'AM' ELSE 'PM' END AS am_pm,
        CASE
            WHEN h.hour < 6 THEN 'Early Morning'
            WHEN h.hour < 12 THEN 'Morning'
            WHEN h.hour < 18 THEN 'Afternoon'
            ELSE 'Evening'
        END AS time_band,
        CASE
            WHEN h.hour < 6 THEN 1
            WHEN h.hour < 12 THEN 2
            WHEN h.hour < 18 THEN 3
            ELSE 4
        END AS shift_id
    FROM generate_series(0, 23) AS h(hour)
    CROSS JOIN generate_series(0, 59) AS m(minute)
)

SELECT * FROM time_entries
