WITH bulletins AS (
	SELECT max(downloaded_at) downloaded_at
	FROM {{ ref('biostatistics_cases') }}
	WHERE downloaded_date >= CURRENT_DATE - INTERVAL '17' DAY
	GROUP BY date(downloaded_at AT TIME ZONE 'America/Puerto_Rico')
)
SELECT
    downloaded_at,
	bulletin_date,
	diagnostic_collected_date collected_date,
	age_gte,
	age_lt,
	municipality,
	count(*) cases,
	count(*) FILTER (
		WHERE case_type = 'Probable'
	) AS probable_cases,
	count(*) FILTER (
		WHERE case_type = 'Confirmed'
	) AS confirmed_cases,
	count(*) FILTER (
		WHERE case_classification = 'Initial'
	) AS initial_cases,
	count(*) FILTER (
		WHERE case_classification = 'Reinfection'
	) AS reinfections
FROM {{ ref('biostatistics_cases') }} cases
INNER JOIN bulletins
	USING (downloaded_at)
LEFT OUTER JOIN {{ ref('biostatistics_deaths_age_ranges') }} ranges
	USING (age_range)
WHERE case_category IN ('Covid19')
AND case_type IN ('Probable', 'Confirmed')
GROUP BY
    downloaded_at,
	bulletin_date,
	diagnostic_collected_date,
	age_gte,
	age_lt,
	municipality
ORDER BY
    downloaded_at,
	collected_date,
	age_gte,
	municipality;