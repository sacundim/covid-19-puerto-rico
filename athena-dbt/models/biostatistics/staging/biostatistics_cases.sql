{{
    config(
        pre_hook=[
            "MSCK REPAIR TABLE {{ source('biostatistics', 'cases_v2').render_hive() }}"
        ],
        table_type='iceberg',
        partitioned_by=['month(bulletin_date)'],
        materialized='incremental',
        incremental_strategy='append',
        post_hook = [
            'VACUUM {{ this.render_pure() }};'
        ]
    )
}}
{% if is_incremental() %}
WITH incremental AS (
    SELECT
        max(downloaded_at) max_downloaded_at,
        CAST(max(downloaded_date) AS VARCHAR) max_downloaded_date
    FROM {{ this }}
)
{% endif %}
SELECT
    date(downloaded_date) AS downloaded_date,
    CAST(downloadedAt AS TIMESTAMP(6))
      AS downloaded_at,
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
    CAST(earliestPositiveRankingTestSampleCollectedDate AS TIMESTAMP(6))
        AS ranking_collected_utc,
    date(earliestPositiveRankingTestSampleCollectedDate AT TIME ZONE 'America/Puerto_Rico')
        AS ranking_collected_date,
    CAST(earliestPositiveDiagnosticTestSampleCollectedDate AS TIMESTAMP(6))
        AS diagnostic_collected_utc,
    date(earliestPositiveDiagnosticTestSampleCollectedDate AT TIME ZONE 'America/Puerto_Rico')
        AS diagnostic_collected_date,
    CAST(caseCreatedAt AS TIMESTAMP(6))
        AS case_created_utc,
    date(caseCreatedAt AT TIME ZONE 'America/Puerto_Rico')
        AS case_created_date
FROM {{ source('biostatistics', 'cases_v2') }}
{% if is_incremental() %}
INNER JOIN incremental
  ON downloadedAt > max_downloaded_at
  AND downloaded_date >= max_downloaded_date
{% endif %}
;