{{ config(
    materialized='table',
    tags=['dimension', 'dbt']
) }}

WITH raw_dates AS (
    SELECT DISTINCT log_date::DATE AS log_date
    FROM {{ source('w3c', 'raw_logs') }}
    WHERE log_date IS NOT NULL
),

date_entries AS (
    SELECT
        TO_CHAR(log_date, 'YYYYMMDD')::INTEGER AS date_sk,
        log_date AS date,
        EXTRACT(YEAR FROM log_date)::INTEGER AS year,
        EXTRACT(MONTH FROM log_date)::INTEGER AS month,
        TO_CHAR(log_date, 'FMMonth') AS month_name,
        EXTRACT(DAY FROM log_date)::INTEGER AS day_number,
        TO_CHAR(log_date, 'FMDay') AS day_name,
        EXTRACT(DOW FROM log_date)::INTEGER AS day_of_week,
        EXTRACT(QUARTER FROM log_date)::INTEGER AS quarter,
        EXTRACT(WEEK FROM log_date)::INTEGER AS week_of_year,
        CASE WHEN EXTRACT(DOW FROM log_date) IN (0, 6) THEN 'Yes' ELSE 'No' END AS is_weekend,
        CASE WHEN log_date IN (
            {% for date in var('uk_holidays') %}'{{ date }}'::DATE{{ ',' if not loop.last }}{% endfor %}
        ) THEN 'Yes' ELSE 'No' END AS holiday_flag
    FROM raw_dates
)

SELECT * FROM date_entries
