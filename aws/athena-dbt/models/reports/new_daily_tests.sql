SELECT
    'Fecha de muestra' AS date_type,
    test_type,
    bulletin_date,
    collected_date AS date,
    tests
FROM {{ ref('bioportal_collected_agg') }}
UNION
SELECT
    'Fecha de reporte' AS date_type,
    test_type,
    bulletin_date,
    reported_date AS date,
    tests
FROM {{ ref('bioportal_reported_agg') }}
ORDER BY bulletin_date DESC, date DESC, test_type, date_type;
