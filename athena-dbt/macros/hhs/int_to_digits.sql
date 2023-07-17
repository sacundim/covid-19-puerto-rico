--
-- A lot of data-driven type inference tools turn zip codes into ints,
-- which removes their leading zeroes.  This macro turns them into strings
-- with leading zeroes.  Works just as well with county FIPS codes.
--
{% macro int_to_digits(x, d) %}
lpad(CAST({{ x }} AS VARCHAR), {{ d }}, '0')
{% endmacro %}