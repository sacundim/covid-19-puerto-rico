--
-- Correct dates that look like they have the correct month and day
-- but wrong year, or correct year and day but incorrect month, etc.
--
{% macro guess_mistyped_field_diff(field, raw_collected_date, received_date) %}
date_diff(
    'day',
    {{raw_collected_date}}
        + INTERVAL (date_diff('{{field}}', {{raw_collected_date}}, {{received_date}})) {{field}},
    {{received_date}}
)
{% endmacro %}
