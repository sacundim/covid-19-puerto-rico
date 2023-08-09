{{
    config(
        pre_hook=[
            "MSCK REPAIR TABLE {{ source('covid19datos_v2', 'pango_lineages').render_hive() }}",
        ]
    )
}}
WITH downloads AS (
  SELECT
    max(downloaded_date) AS downloaded_date,
    max(downloaded_at) AS downloaded_at
  FROM {{ source('covid19datos_v2', 'pango_lineages') }}
)
SELECT
  CAST(downloaded_at AS TIMESTAMP(6)) AS downloaded_at,
  date(downloaded_at AT TIME ZONE 'America/Puerto_Rico' - INTERVAL '1' DAY)
    AS bulletin_date,
  lineage,
  unaliased,
  root,
  numbers,
  description
FROM {{ source('covid19datos_v2', 'pango_lineages') }}
INNER JOIN downloads
  USING (downloaded_date, downloaded_at)