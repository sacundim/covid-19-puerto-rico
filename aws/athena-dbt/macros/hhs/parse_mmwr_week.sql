--
-- Parse `mmwr_week` strings in the format `202138`.  Evaluates to
-- the first date of the week.
--
{% macro parse_mmwr_week(mmwr_week) %}
date_add('day', -1, date(parse_datetime({{ mmwr_week }}, 'xxxxww')))
{% endmacro %}
