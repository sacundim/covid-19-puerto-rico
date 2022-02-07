{% test sequential_values(model, column_name, interval=1, datepart=None, partition_by=None) %}

with windowed as (
    select
        {% if partition_by %}
        {{ partition_by | join(', ') }},
        {% endif %}
        {{ column_name }},
        lag({{ column_name }}) over (
            {% if partition_by %}
            partition by {{ partition_by | join(', ') }}
            {% endif %}
            order by {{ column_name }}
        ) as previous_{{ column_name }}
    from {{ model }}
),
validation_errors as (
    select
        *
    from windowed
    {% if datepart %}
    where {{ column_name }} != date_add('{{ datepart }}', {{ interval }}, previous_{{ column_name }})
    {% else %}
    where not({{ column_name }} = previous_{{ column_name }} + {{ interval }})
    {% endif %}
)
select *
from validation_errors

{% endtest %}