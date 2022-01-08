--
-- Expression to clean the `testType` field.
--
{% macro clean_test_type(raw) %}
    CASE
        WHEN {{raw}} IN (
            'Molecular', 'MOLECULAR'
        ) THEN 'Molecular'
        WHEN {{raw}} IN (
            'Antigens', 'ANTIGENO'
        ) THEN 'Antígeno'
        WHEN {{raw}} IN (
            'Serological', 'Serological IgG Only', 'Total Antibodies', 'SEROLOGICAL'
        ) THEN 'Serológica'
        ELSE {{raw}}
    END
{% endmacro %}
