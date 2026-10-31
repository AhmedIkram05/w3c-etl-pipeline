{% macro tsql_cast(field, type) -%}
  {% if target.type == 'sqlserver' -%}
    CAST({{ field }} AS {{ type }})
  {%- else -%}
    {{ field }}::{{ type }}
  {%- endif %}
{%- endmacro %}

{% macro tsql_datepart(part, field) -%}
  {% if target.type == 'sqlserver' -%}
    DATEPART({{ part }}, {{ field }})
  {%- else -%}
    EXTRACT({{ part }} FROM {{ field }})
  {%- endif %}
{%- endmacro %}

{% macro tsql_month_name(field) -%}
  {% if target.type == 'sqlserver' -%}
    DATENAME(month, {{ field }})
  {%- else -%}
    TO_CHAR({{ field }}, 'FMMonth')
  {%- endif %}
{%- endmacro %}

{% macro tsql_day_name(field) -%}
  {% if target.type == 'sqlserver' -%}
    DATENAME(weekday, {{ field }})
  {%- else -%}
    TO_CHAR({{ field }}, 'FMDay')
  {%- endif %}
{%- endmacro %}

{% macro tsql_dow(field) -%}
  {% if target.type == 'sqlserver' -%}
    (DATEDIFF(day, '19000107', {{ field }}) % 7)
  {%- else -%}
    EXTRACT(dow FROM {{ field }})
  {%- endif %}
{%- endmacro %}

{% macro tsql_iso_week(field) -%}
  {% if target.type == 'sqlserver' -%}
    DATEPART(ISO_WEEK, {{ field }})
  {%- else -%}
    EXTRACT(week FROM {{ field }})
  {%- endif %}
{%- endmacro %}

{% macro tsql_format_date(field, format) -%}
  {% if target.type == 'sqlserver' -%}
    {% if format == 'YYYY-MM-DD' -%}
      FORMAT({{ field }}, 'yyyy-MM-dd')
    {%- elif format == 'YYYY-MM' -%}
      FORMAT({{ field }}, 'yyyy-MM')
    {%- elif format == 'YYYY' -%}
      FORMAT({{ field }}, 'yyyy')
    {%- elif format == 'YYYYMMDD' -%}
      FORMAT({{ field }}, 'yyyyMMdd')
    {%- elif format == 'FMMonth' -%}
      DATENAME(month, {{ field }})
    {%- elif format == 'FMDay' -%}
      DATENAME(weekday, {{ field }})
    {%- else -%}
      FORMAT({{ field }}, '{{ format }}')
    {%- endif %}
  {%- else -%}
    TO_CHAR({{ field }}, '{{ format }}')
  {%- endif %}
{%- endmacro %}

{% macro tsql_split_part(field, delimiter, part) -%}
  {% if target.type == 'sqlserver' -%}
    -- NOTE: Only parts 1 and 2 are implemented.
    -- Parts >= 3 return a 0-length string (silently).
    CASE
      WHEN CHARINDEX('{{ delimiter }}', {{ field }}) = 0 THEN {{ field }}
      ELSE SUBSTRING(
        {{ field }},
        CASE {{ part }}
          WHEN 1 THEN 1
          ELSE CHARINDEX('{{ delimiter }}', {{ field }}) + 1
        END,
        CASE {{ part }}
          WHEN 1 THEN CHARINDEX('{{ delimiter }}', {{ field }}) - 1
          WHEN 2 THEN LEN({{ field }}) - CHARINDEX('{{ delimiter }}', {{ field }})
          ELSE 0
        END
      )
    END
  {%- else -%}
    SPLIT_PART({{ field }}, '{{ delimiter }}', {{ part }})
  {%- endif %}
{%- endmacro %}

{% macro tsql_generate_series(start, end, step=1) -%}
  {% if target.type == 'sqlserver' -%}
    GENERATE_SERIES({{ start }}, {{ end }}, {{ step }})
  {%- else -%}
    generate_series({{ start }}, {{ end }}, {{ step }})
  {%- endif %}
{%- endmacro %}

{% macro tsql_percentile_cont(percent, field) -%}
  {% if target.type == 'sqlserver' -%}
    PERCENTILE_CONT({{ percent }}) WITHIN GROUP (ORDER BY {{ field }}) OVER ()
  {%- else -%}
    PERCENTILE_CONT({{ percent }}) WITHIN GROUP (ORDER BY {{ field }}) OVER ()
  {%- endif %}
{%- endmacro %}

{% macro tsql_create_index_if_not_exists(table_name, index_name, columns, unique=false) -%}
  {% if target.type == 'sqlserver' -%}
    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = '{{ index_name }}' AND object_id = OBJECT_ID('{{ table_name }}'))
    BEGIN
      CREATE {% if unique %}UNIQUE {% endif %}INDEX {{ index_name }} ON {{ table_name }} ({{ columns }})
    END;
  {%- else -%}
    CREATE INDEX IF NOT EXISTS {{ index_name }} ON {{ table_name }} ({{ columns }})
  {%- endif %}
{%- endmacro %}

{% macro tsql_hash_md5(concat_expr) -%}
  {% if target.type == 'sqlserver' -%}
    CONVERT(VARCHAR(32), HASHBYTES('MD5', {{ concat_expr }}), 2)
  {%- else -%}
    MD5({{ concat_expr }})
  {%- endif %}
{%- endmacro %}

{% macro tsql_ua_hash(field) -%}
  {#- MD5 of the FULL raw user-agent string, used to join fact -> dim_useragent.
      Casing matches the Python dim builders (lowercase md5): PostgreSQL MD5()
      is already lowercase; SQL Server HASHBYTES returns uppercase, so LOWER().
      On SQL Server the input is cast to VARCHAR (code-page bytes) because
      HASHBYTES over nvarchar uses UTF-16 — for IIS user-agents (URL-encoded
      ASCII) the code-page bytes equal the UTF-8 bytes Python hashes. -#}
  {% if target.type == 'sqlserver' -%}
    LOWER(CONVERT(VARCHAR(32), HASHBYTES('MD5', CONVERT(VARCHAR(MAX), {{ field }})), 2))
  {%- else -%}
    MD5({{ field }})
  {%- endif -%}
{%- endmacro %}

{% macro tsql_boolean_to_int(field) -%}
  {% if target.type == 'sqlserver' -%}
    CASE WHEN {{ field }} = 1 THEN 1 ELSE 0 END
  {%- else -%}
    CASE WHEN {{ field }} THEN 1 ELSE 0 END
  {%- endif %}
{%- endmacro %}

{% macro tsql_bool_literal(val) -%}
  {% if target.type == 'sqlserver' -%}
    {% if val %}1{% else %}0{% endif %}
  {%- else -%}
    {% if val %}TRUE{% else %}FALSE{% endif %}
  {%- endif %}
{%- endmacro %}

{% macro tsql_true_val() -%}
  {% if target.type == 'sqlserver' -%}1{%- else -%}TRUE{%- endif %}
{%- endmacro %}

{% macro tsql_false_val() -%}
  {% if target.type == 'sqlserver' -%}0{%- else -%}FALSE{%- endif %}
{%- endmacro %}

{% macro sqlserver__test_expression_is_true(model, expression, column_name) %}
{% set column_list = '*' if should_store_failures() else "1 as is_valid" %}
select
    {{ column_list }}
from {{ model }}
{% if column_name is none %}
where not({{ expression }})
{%- else %}
where not({{ column_name }} {{ expression }})
{%- endif %}
{% endmacro %}

{% macro sqlserver__collect_freshness(source, loaded_at_field, filter) %}
  {% call statement('collect_freshness', fetch_result=True, auto_begin=False) -%}
    select
      max(CAST({{ loaded_at_field }} AS DATETIME)) as max_loaded_at,
      {{ current_timestamp() }} as snapshotted_at
    from {{ source }}
    {% if filter %}
    where {{ filter }}
    {% endif %}
  {% endcall %}
  {{ return(load_result('collect_freshness')) }}
{% endmacro %}

{% macro tsql_extract_domain(url_field) -%}
  {% if target.type == 'sqlserver' -%}
    LOWER(
      CASE
        -- A host segment is required. 'https://' with no host, or a leading
        -- '/' after the scheme, must fall through to the ELSE so the result
        -- matches the PostgreSQL branch (whose anchored regexp leaves a
        -- host-less input unchanged rather than returning an empty string).
        WHEN {{ url_field }} LIKE 'https://%'
          AND CHARINDEX('/', {{ url_field }} + '/', 9) > 9
          THEN SUBSTRING({{ url_field }}, 9, CHARINDEX('/', {{ url_field }} + '/', 9) - 9)
        WHEN {{ url_field }} LIKE 'http://%'
          AND CHARINDEX('/', {{ url_field }} + '/', 8) > 8
          THEN SUBSTRING({{ url_field }}, 8, CHARINDEX('/', {{ url_field }} + '/', 8) - 8)
        ELSE {{ url_field }}
      END
    )
  {%- else -%}
    LOWER(REGEXP_REPLACE({{ url_field }}, '^https?://([^/]+).*', '\1'))
  {%- endif %}
{%- endmacro %}
