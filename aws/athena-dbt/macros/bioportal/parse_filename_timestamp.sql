--
-- We enrich a lot of our downloaded file names with timestamps like this:
--
--     orders-basic_2020-11-10T09:50:53Z.parquet
--
-- This macro parses the 2020-11-10T09:50:53Z part into a timestamp.
--
{% macro parse_filename_timestamp(path) %}
CAST(from_iso8601_timestamp(
		regexp_extract({{ path }}, '(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})Z'))
		AS TIMESTAMP(6))
{% endmacro %}
