--
-- For privacy reasons, the HHS data set puts -999999 for values
-- that are less than four, but still reports zeroes as zeroes.
-- They do this superficially for the `*_7_day_avg` columns as for
-- the `*_7_day_sum`, which means that you can get more precise
-- averages by not using `*_7_day_avg` at all and instead dividing
-- the `*_7_day_sum` by the `*_7_day_coverage` (number of days the
-- facility reported in that week).
--
-- By imputing 0.0 and 4.0 respectively we can also obtain a lower
-- and upper bound for omitted sums, and we provide functions for
-- that as well.
--
{% macro hhs_avg(sum, coverage) %}
    CAST(nullif({{sum}}, -999999) AS DOUBLE PRECISION)
{% endmacro %}
