{{
    config(pre_hook=[
        "MSCK REPAIR TABLE {{ source('biostatistics', 'data_sources_v2').render_hive() }}"
    ])
}}
SELECT
    date(downloaded_date) AS downloaded_date,
    downloadedAt AS downloaded_at,
    CAST(downloadedAt AT TIME ZONE 'America/Puerto_Rico' AS DATE)
        - INTERVAL '1' DAY
        AS bulletin_date,
    id,
    name,
    recordCount AS record_count,
    lastUpdated AS last_updated_utc,
    originTimeZone AS origin_timezone
FROM {{ source('biostatistics', 'data_sources_v2') }}
ORDER BY downloaded_at, last_updated_utc;