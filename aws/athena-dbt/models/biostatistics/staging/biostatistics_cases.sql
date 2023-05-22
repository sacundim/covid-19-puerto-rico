{{
    config(pre_hook=[
        "MSCK REPAIR TABLE {{ source('biostatistics', 'cases_v2').render_hive() }}"
    ])
}}
SELECT
    date(downloaded_date) AS downloaded_date,
    downloadedAt AS downloaded_at,
    CAST(downloadedAt AT TIME ZONE 'America/Puerto_Rico' AS DATE)
        - INTERVAL '1' DAY
        AS bulletin_date,
    caseId AS case_id,
    caseCategory AS case_category,
    caseType AS case_type,
    caseClassification AS case_classification,
    from_hex(replace(nullif(patientId, ''), '-')) AS patient_id,
    {{ clean_age_range('patientAgeRange') }} AS age_range,
    patientSex AS sex,
    {{ clean_region('patientPhysicalRegion') }} AS region,
    {{ clean_municipality('patientPhysicalCity') }} AS municipality,
    earliestPositiveRankingTestSampleCollectedDate
        AS ranking_collected_utc,
    date(earliestPositiveRankingTestSampleCollectedDate AT TIME ZONE 'America/Puerto_Rico')
        AS ranking_collected_date,
    earliestPositiveDiagnosticTestSampleCollectedDate
        AS diagnostic_collected_utc,
    date(earliestPositiveDiagnosticTestSampleCollectedDate AT TIME ZONE 'America/Puerto_Rico')
        AS diagnostic_collected_date,
    caseCreatedAt AS case_created_utc,
    date(caseCreatedAt AT TIME ZONE 'America/Puerto_Rico')
        AS case_created_date
FROM {{ source('biostatistics', 'cases_v2') }}
ORDER BY
    downloaded_at,
    diagnostic_collected_utc;