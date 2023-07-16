--
-- Correct dates that look like they have the correct month and day
-- but wrong year, or correct year and day but incorrect month, etc.
--
{% macro guess_mistyped_field_diff(field, raw_collected_date, received_date) %}
date_diff(
    'day',
    date_add(
        '{{field}}',
        date_diff('{{field}}', {{raw_collected_date}}, {{received_date}}),
        {{raw_collected_date}}
    ),
    {{received_date}}
)
{% endmacro %}
