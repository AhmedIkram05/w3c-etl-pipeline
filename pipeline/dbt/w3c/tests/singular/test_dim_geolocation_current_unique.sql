{{ config(severity='error') }}

-- Singular test: the SCD Type 2 geo dimension must have at most ONE current
-- version per ip.
--
-- The writer keeps this true in two steps (expire the changed current row, then
-- insert its replacement) and the partial unique index
-- ux_dim_geolocation__current (ip WHERE is_current) enforces it. This test
-- catches a reader/writer drift that would leave two current rows for one IP
-- and fan out fact_webrequest's LEFT JOIN to dim_geolocation.

SELECT
    ip,
    COUNT(*) AS current_versions
FROM {{ source('w3c', 'dim_geolocation') }}
WHERE {% if target.type == 'sqlserver' %}is_current = 1{% else %}is_current{% endif %}
GROUP BY ip
HAVING COUNT(*) > 1
