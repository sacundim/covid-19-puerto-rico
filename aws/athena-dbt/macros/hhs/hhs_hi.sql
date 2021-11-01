{% macro hhs_hi(sum, coverage) %}
CASE WHEN {{sum}} = -999999 THEN 4.0 ELSE CAST({{sum}} AS DOUBLE PRECISION) / {{coverage}} END
{% endmacro %}
