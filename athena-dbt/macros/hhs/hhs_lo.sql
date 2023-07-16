{% macro hhs_lo(sum, coverage) %}
CASE WHEN {{sum}} = -999999 THEN 0.0 ELSE CAST({{sum}} AS DOUBLE PRECISION) / {{coverage}} END
{% endmacro %}
