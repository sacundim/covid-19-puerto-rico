--
-- HHS dataset files have names like this:
--
--     covid_vaccinations_county_20210613_1841.parquet
--
-- This macro parses the 20210613_1841 part into a timestamp.  In Athena
-- you typically want to pass '"$path"' as the argument.
--
{% macro hhs_parse_filename_date(path) %}
date_parse(regexp_extract({{ path }}, '202[0123](\d{4})_(\d{4})'), '%Y%m%d_%H%i')
{% endmacro %}
