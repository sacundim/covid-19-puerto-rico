--
-- Expression to parse a Bioportal result.  We keep a join
-- table of known results elsewhere that we pass in as
-- `lookup_result`, otherwise we parse
--
{% macro parse_bioportal_result(raw_result, lookup_result) %}
    CASE WHEN {{lookup_result}} IS NOT NULL
         THEN cast({{lookup_result}} AS BOOLEAN)
         ELSE NOT regexp_matches(COALESCE({{raw_result}}), '(?i)influenza')
                AND COALESCE({{raw_result}}, '') LIKE '%Positive%'
    END
{% endmacro %}
