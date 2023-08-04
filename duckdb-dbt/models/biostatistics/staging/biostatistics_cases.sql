SELECT
    CAST(downloaded_date AS DATE) AS downloaded_date,
    downloadedAt AS downloaded_at,
    CAST(downloadedAt AT TIME ZONE 'America/Puerto_Rico' AS DATE)
        - INTERVAL 1 DAY
        AS bulletin_date,
    caseId AS case_id,
    caseCategory AS case_category,
    caseType AS case_type,
    caseClassification AS case_classification,
    CAST(patientId AS UUID) AS patient_id,
    {{ clean_age_range('patientAgeRange') }} AS age_range,
    patientSex AS sex,
    {{ clean_region('patientPhysicalRegion') }} AS region,
    {{ clean_municipality('patientPhysicalCity') }} AS municipality,
    earliestPositiveRankingTestSampleCollectedDate
        AS ranking_collected_utc,
    CAST(earliestPositiveRankingTestSampleCollectedDate AT TIME ZONE 'America/Puerto_Rico' AS DATE)
        AS ranking_collected_date,
    earliestPositiveDiagnosticTestSampleCollectedDate
        AS diagnostic_collected_utc,
    CAST(earliestPositiveDiagnosticTestSampleCollectedDate AT TIME ZONE 'America/Puerto_Rico' AS DATE)
        AS diagnostic_collected_date,
    caseCreatedAt
        AS case_created_utc,
    CAST(caseCreatedAt AT TIME ZONE 'America/Puerto_Rico' AS DATE)
        AS case_created_date
FROM {{ source('biostatistics', 'cases') }}