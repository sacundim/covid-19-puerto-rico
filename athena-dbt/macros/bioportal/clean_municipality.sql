--
-- Expression to clean a municipality field.
--
{% macro clean_municipality(raw) %}
    CASE {{raw}}
        WHEN '' THEN NULL
        WHEN 'Loiza' THEN 'Loíza'
        WHEN 'Rio Grande' THEN 'Río Grande'
        ELSE {{raw}}
    END
{% endmacro %}
