{{
    config(pre_hook=[
        "MSCK REPAIR TABLE {{ source('biostatistics', 'tests_grouped_v2').render_hive() }}"
    ])
}}
SELECT
    date(downloaded_date) AS downloaded_date,
    CAST(downloadedAt AS TIMESTAMP(6))
        AS downloaded_at,
    CAST(downloadedAt AT TIME ZONE 'America/Puerto_Rico' AS DATE)
        - INTERVAL '1' DAY
        AS bulletin_date,
    CAST(sampleCollectedDate AS TIMESTAMP(6))
        AS collected_date,
    entity,
    {{ clean_municipality('entityCity') }} AS entity_city,
    totalTestsProcessed AS total_tests_processed,
    totalMolecularTestsProcessed AS total_molecular_tests_processed,
    totalMolecularTestsPositive AS total_molecular_tests_positive,
    totalMolecularTestsNegative AS total_molecular_tests_negtive,
    totalAntigensTestsProcessed AS total_antigens_tests_processed,
    totalAntigensTestsPositive AS total_antigens_tests_positive,
    totalAntigensTestsNegative AS total_antigens_tests_negative
FROM {{ source('biostatistics', 'tests_grouped_v2') }};