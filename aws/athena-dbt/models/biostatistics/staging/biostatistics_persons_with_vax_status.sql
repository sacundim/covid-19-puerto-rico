{{
    config(pre_hook=[
        "MSCK REPAIR TABLE {{ source('biostatistics', 'persons_with_vax_status_v2').render_hive() }}"
    ])
}}
SELECT
    date(downloaded_date) AS downloaded_date,
    CAST(downloadedAt AS TIMESTAMP(6))
        AS downloaded_at,
    CAST(downloadedAt AT TIME ZONE 'America/Puerto_Rico' AS DATE)
        - INTERVAL '1' DAY
        AS bulletin_date,
    {{ clean_age_range('personAgeRange') }} AS age_range,
    personSex AS sex,
    personState AS state,
    {{ clean_region('personRegion') }} AS region,
    {{ clean_municipality('personCity') }} AS municipality,
    personVaccinationStatus AS vax_status,
    personLastVaccinationDate AS last_vax_date
FROM {{ source('biostatistics', 'persons_with_vax_status_v2') }};