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
    DATEPART(weekday, {{ field }}) - 1
  {%- else -%}
    EXTRACT(dow FROM {{ field }})
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

{% macro tsql_regexp_replace(field, pattern, replacement) -%}
  {% if target.type == 'sqlserver' -%}
    {{ field }}
  {%- else -%}
    REGEXP_REPLACE({{ field }}, '{{ pattern }}', '{{ replacement }}')
  {%- endif %}
{%- endmacro %}

{% macro tsql_case_insensitive_like(field, pattern) -%}
  {% if target.type == 'sqlserver' -%}
    {{ field }} LIKE '{{ pattern }}' COLLATE SQL_Latin1_General_CP1_CI_AS
  {%- else -%}
    {{ field }} ~* '{{ pattern }}'
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

{% macro tsql_extract_domain(url_field) -%}
  {% if target.type == 'sqlserver' -%}
    LOWER(
      CASE
        WHEN {{ url_field }} LIKE 'https://%'
          THEN SUBSTRING({{ url_field }}, 9, CHARINDEX('/', {{ url_field }} + '/', 9) - 9)
        WHEN {{ url_field }} LIKE 'http://%'
          THEN SUBSTRING({{ url_field }}, 8, CHARINDEX('/', {{ url_field }} + '/', 8) - 8)
        ELSE {{ url_field }}
      END
    )
  {%- else -%}
    LOWER(REGEXP_REPLACE({{ url_field }}, '^https?://([^/]+).*', '\1'))
  {%- endif %}
{%- endmacro %}
