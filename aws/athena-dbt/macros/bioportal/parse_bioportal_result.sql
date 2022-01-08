--
-- Expression to parse a Bioportal result.
--
{% macro parse_bioportal_result(raw) %}
    NOT regexp_like(COALESCE({{raw}}), '(?i)influenza')
        AND COALESCE({{raw}}, '') LIKE '%Positive%'
{% endmacro %}
