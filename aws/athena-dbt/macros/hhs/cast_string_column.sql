{% macro cast_string_column(column, type) %}
    CAST(nullif("{{column}}", '') AS {{type}}) AS "{{column}}"
{% endmacro %}
