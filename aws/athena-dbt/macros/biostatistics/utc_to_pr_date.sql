--
-- Expression to convert an UTC timestamp to a date in Puerto Rico.
--
{% macro utc_to_pr_date(raw) %}
date(from_iso8601_timestamp(nullif({{raw}}, '')) AT TIME ZONE 'America/Puerto_Rico')
{% endmacro %}
