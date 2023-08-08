{{
    config(
        pre_hook=[
            "MSCK REPAIR TABLE {{ source('covid19datos_v2', 'vigilancia_v4').render_hive() }}",
        ]
    )
}}
SELECT
  CAST(downloaded_at AS TIMESTAMP(6))
    AS downloaded_at,
  downloaded_date,
  date(downloaded_at AT TIME ZONE 'America/Puerto_Rico' - INTERVAL '1' DAY)
    AS bulletin_date,
  date(FE_COLECCION) AS collected_date,
  date(FE_REPORTE) AS report_date,
  TX_LINEAJE AS lineage
FROM {{ source('covid19datos_v2', 'vigilancia_v4') }}