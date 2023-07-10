SELECT
    bulletin_date,
    collected_date,
    delta_specimens
        AS delta_tests,
    delta_positive_specimens
        AS delta_positive_tests
FROM {{ ref('biostatistics_specimens_collected_agg') }}
WHERE test_type = 'Molecular'