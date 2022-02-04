--
-- Expression to clean a region field.
--
{% macro clean_region(raw) %}
    CASE {{raw}}
        WHEN '' THEN NULL
        WHEN 'Bayamon' THEN 'Bayamón'
        WHEN 'Mayaguez' THEN 'Mayagüez'
        ELSE {{raw}}
    END
{% endmacro %}
