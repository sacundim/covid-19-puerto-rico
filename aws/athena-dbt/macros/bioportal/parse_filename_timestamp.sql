--
-- We enrich a lot of our downloaded file names with timestamps like this:
--
--     orders-basic_2020-11-10T09:50:53.parquetInfo
--
-- This macro parses the 2020-11-10T09:50:53 part into a timestamp.
--
{% macro parse_filename_timestamp(path) %}
CAST(from_iso8601_timestamp(
		regexp_extract({{ path }}, '(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})Z'))
		AS TIMESTAMP)
{% endmacro %}
