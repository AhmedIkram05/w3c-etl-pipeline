{{ config(
    materialized='table',
    tags=['dimension', 'dbt']
) }}

WITH raw_dates AS (
    SELECT DISTINCT {{ tsql_cast('log_date', 'DATE') }} AS log_date
    FROM {{ source('w3c', 'raw_enriched') }}
    WHERE log_date IS NOT NULL
),

date_entries AS (
    SELECT
        {{ tsql_cast(tsql_format_date('log_date', 'YYYYMMDD'), 'INT') }} AS date_sk,
        log_date AS date,
        {{ tsql_cast(tsql_datepart('year', 'log_date'), 'INT') }} AS year,
        {{ tsql_cast(tsql_datepart('month', 'log_date'), 'INT') }} AS month,
        {{ tsql_month_name('log_date') }} AS month_name,
        {{ tsql_cast(tsql_datepart('day', 'log_date'), 'INT') }} AS day_number,
        {{ tsql_day_name('log_date') }} AS day_name,
        {{ tsql_cast(tsql_dow('log_date'), 'INT') }} AS day_of_week,
        {{ tsql_cast(tsql_datepart('quarter', 'log_date'), 'INT') }} AS quarter,
        {{ tsql_cast(tsql_datepart('week', 'log_date'), 'INT') }} AS week_of_year,
        CASE WHEN {{ tsql_dow('log_date') }} IN (0, 6) THEN 'Yes' ELSE 'No' END AS is_weekend,
        CASE WHEN log_date IN (
            {% for date in var('uk_holidays') %}
                {% if target.type == 'sqlserver' %}CAST('{{ date }}' AS DATE){% else %}'{{ date }}'::DATE{% endif %}{{ ',' if not loop.last }}
            {% endfor %}
        ) THEN 'Yes' ELSE 'No' END AS holiday_flag
    FROM raw_dates
)

SELECT * FROM date_entries
