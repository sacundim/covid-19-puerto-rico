SELECT
    downloaded_at,
    bulletin_date,
    collected_date,
    municipality,
    pop2020 population,
    sum(specimens) AS specimens,
    coalesce(sum(specimens) FILTER (
        WHERE test_type = 'Antígeno'
    ), 0) AS antigens,
    coalesce(sum(specimens) FILTER (
        WHERE test_type = 'Molecular'
    ), 0) AS molecular,
    sum(cumulative_specimens) AS cumulative_specimens,
    coalesce(sum(cumulative_specimens) FILTER (
        WHERE test_type = 'Antígeno'
    ), 0) AS cumulative_antigens,
    coalesce(sum(cumulative_specimens) FILTER (
        WHERE test_type = 'Molecular'
    ), 0) AS cumulative_molecular
FROM {{ ref('municipal_tests_collected_agg') }} tests
INNER JOIN {{ ref('municipal_population') }} muni
	ON muni.name = tests.municipality
WHERE test_type IN ('Antígeno', 'Molecular')
GROUP BY
    downloaded_at,
    bulletin_date,
    municipality,
    collected_date,
    pop2020
ORDER BY
    downloaded_at,
    bulletin_date,
    municipality,
    collected_date;
