--
-- Expression to clean an `age_range` field.
--
{% macro clean_age_range(raw) %}
    CASE {{raw}}
        WHEN '' THEN NULL
        WHEN 'N/A' THEN NULL
        ELSE {{raw}}
    END
{% endmacro %}
