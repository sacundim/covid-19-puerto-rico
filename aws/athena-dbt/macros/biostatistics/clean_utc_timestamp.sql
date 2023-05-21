--
-- Expression to convert an UTC timestamp string to timestamp.
--
-- NOTE: Athena has weird behavior in that
--
-- 1. Its query engine (Trino) supports TIMESTAMP WITH TIME ZONE
--
-- but
--
-- 2. Its storage engine (Hive) doesn't
--
-- And that's why we have to convert to TIMESTAMP WITHOUT TIME ZONE
--
{% macro clean_utc_timestamp(raw) %}
CAST(from_iso8601_timestamp(nullif({{raw}}, '')) AS TIMESTAMP(3) WITHOUT TIME ZONE)
{% endmacro %}
