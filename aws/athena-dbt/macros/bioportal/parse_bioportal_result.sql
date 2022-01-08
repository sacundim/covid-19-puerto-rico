--
-- Expression to parse a Bioportal result.
--
{% macro parse_bioportal_result(raw) %}
    COALESCE({{raw}}, '') LIKE '%Positive%'
{% endmacro %}
