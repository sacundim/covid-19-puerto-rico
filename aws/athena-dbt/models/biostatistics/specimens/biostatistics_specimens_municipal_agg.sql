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
    coalesce(sum(positive_specimens) FILTER (
        WHERE test_type = 'Antígeno'
    ), 0) AS positive_antigens,
    coalesce(sum(specimens) FILTER (
        WHERE test_type = 'Molecular'
    ), 0) AS molecular,
    coalesce(sum(positive_specimens) FILTER (
        WHERE test_type = 'Molecular'
    ), 0) AS positive_molecular,
    sum(sum(specimens)) OVER cumulative
        AS cumulative_specimens,
    sum(sum(specimens) FILTER (
        WHERE test_type = 'Antígeno'
    )) OVER cumulative
        AS cumulative_antigens,
    sum(sum(specimens) FILTER (
        WHERE test_type = 'Molecular'
    )) OVER cumulative
        AS cumulative_molecular
FROM {{ ref('biostatistics_specimens_cube') }} specimens
INNER JOIN {{ ref('municipal_population') }} muni
	ON muni.name = specimens.municipality
WHERE test_type IN ('Antígeno', 'Molecular')
GROUP BY
    downloaded_at,
    bulletin_date,
    municipality,
    collected_date,
    pop2020
WINDOW cumulative AS (
    PARTITION BY downloaded_at, municipality
    ORDER BY collected_date
)
ORDER BY
    downloaded_at,
    bulletin_date,
    municipality,
    collected_date;