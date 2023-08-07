WITH bulletins AS (
  SELECT
    date(downloaded_at AT TIME ZONE 'America/Puerto_Rico') - INTERVAL '1' DAY
      AS bulletin_date,
    max(downloaded_at) AS downloaded_at
  FROM covid19datos_v2_sources.vigilancia_parquet_v4
  GROUP BY date(downloaded_at AT TIME ZONE 'America/Puerto_Rico')
)
SELECT
	downloaded_at,
	bulletin_date,
	collected_date,
	date(parse_datetime(
		CAST(year_of_week(collected_date) AS VARCHAR)
			|| '-'
			|| CAST(week(collected_date) AS VARCHAR),
		'xxxx-ww'
	) + INTERVAL '6' DAY) AS week_ending,
	pango_lineage,
	count(*) count
FROM {{ ref('vigilancia') }}
INNER JOIN bulletins
  USING (downloaded_at)
GROUP BY
  downloaded_at,
  bulletin_date,
  collected_date,
  pango_lineage
