WITH bulletins AS (
	SELECT max(downloaded_at) downloaded_at
	FROM {{ ref('biostatistics_deaths') }}
	GROUP BY CAST(downloaded_at AT TIME ZONE 'America/Puerto_Rico' AS DATE)
)
SELECT
	downloaded_at,
	downloaded_date,
	bulletin_date,
	death_date,
	age_range,
	age_gte,
	age_lt,
	region,
	vaccination_status_at_death,
	count(*) deaths
FROM {{ ref('biostatistics_deaths') }}
INNER JOIN bulletins
	USING (downloaded_at)
INNER JOIN {{ ref('biostatistics_deaths_age_ranges') }}
	USING (age_range)
WHERE death_date <= bulletin_date
GROUP BY
	downloaded_at,
	downloaded_date,
	bulletin_date,
	death_date,
	age_range,
	age_gte,
	age_lt,
	region,
	vaccination_status_at_death
ORDER BY
	downloaded_at,
	death_date,
	age_gte,
	region