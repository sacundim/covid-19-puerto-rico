{{
    config(
        pre_hook=[
            "MSCK REPAIR TABLE {{ source('biostatistics', 'flu_cases_v2').render_hive() }}"
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
    {{ clean_age_range('patientAgeRange') }} AS age_range,
    patientSex AS sex,
    {{ clean_municipality('patientPhysicalCity') }} AS municipality,
    identifyingTestSampleCollectedDate AS collected_date
FROM {{ source('biostatistics', 'flu_cases_v2') }}
{% if is_incremental() %}
INNER JOIN incremental
  ON downloadedAt > max_downloaded_at
  AND downloaded_date >= max_downloaded_date
{% endif %}
;