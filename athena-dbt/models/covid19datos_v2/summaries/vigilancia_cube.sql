WITH bulletins AS (
  SELECT
    date(downloaded_at AT TIME ZONE 'America/Puerto_Rico') - INTERVAL '1' DAY
      AS bulletin_date,
    max(downloaded_at) AS downloaded_at
  FROM {{ source('covid19datos_v2', 'vigilancia_v4') }}
  GROUP BY date(downloaded_at AT TIME ZONE 'America/Puerto_Rico')
)
SELECT
	vigilancia.bulletin_date,
	collected_date,
	from_iso8601_date(
		CAST(year_of_week(collected_date) AS VARCHAR)
			|| '-W'
			|| CAST(week(collected_date) AS VARCHAR)
	) AS week_starting,
	lineage,
	count(*) count
FROM {{ ref('vigilancia') }} vigilancia
INNER JOIN bulletins
  USING (downloaded_at)
INNER JOIN {{ ref('pango_lineages') }}
  USING (lineage)
GROUP BY
  vigilancia.bulletin_date,
  collected_date,
  lineage