SELECT
    'Fecha de muestra' AS date_type,
    test_type,
    bulletin_date,
    collected_date AS date,
    specimens AS tests
FROM {{ ref('biostatistics_specimens_collected_agg') }}
UNION
SELECT
    'Fecha de reporte' AS date_type,
    test_type,
    bulletin_date,
    reported_date AS date,
    specimens AS tests
FROM {{ ref('biostatistics_specimens_reported_agg') }}
ORDER BY bulletin_date, date, test_type, date_type