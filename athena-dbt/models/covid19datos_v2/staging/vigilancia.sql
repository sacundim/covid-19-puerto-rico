{{
    config(
        pre_hook=[
            "MSCK REPAIR TABLE {{ source('covid19datos_v2', 'vigilancia_v4').render_hive() }}",
        ]
    )
}}
SELECT
  downloaded_date,
  CAST(downloaded_at AS TIMESTAMP(6))
    AS downloaded_at,
  date(FE_COLECCION) AS collected_date,
  date(FE_REPORTE) AS report_date,
  TX_LINEAJE AS pango_lineage
FROM {{ source('covid19datos_v2', 'vigilancia_v4') }}