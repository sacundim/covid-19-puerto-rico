--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
--
-- Cases by municipality.
--

WITH downloads AS (
    SELECT
        date_add('day', -1, date(downloaded_at AT TIME ZONE 'America/Puerto_Rico'))
            AS bulletin_date,
        max(downloaded_at) downloaded_at
    FROM {{ ref('casos') }}
    GROUP BY date_add('day', -1, date(downloaded_at AT TIME ZONE 'America/Puerto_Rico'))
), grid AS (
	SELECT
		bulletin_date,
		date(date_column) AS sample_date,
		city,
		display_name,
		fips
	FROM {{ ref('covid19datos_v2_casos_city_names') }}
	CROSS JOIN (
		VALUES (SEQUENCE(DATE '2020-03-09', DATE '2021-12-31', INTERVAL '1' DAY))
	) AS date_array (date_array)
	CROSS JOIN UNNEST(date_array) AS t2(date_column)
	INNER JOIN downloads
		ON CAST(date_column AS DATE) < bulletin_date
)
SELECT
	grid.bulletin_date,
	grid.sample_date,
	popest.region,
	display_name municipality,
	fips,
	pop2020,
	count(casos.downloaded_at) new_cases,
	count(casos.downloaded_at) FILTER (
		WHERE class = 'CONFIRMADO'
	) AS new_confirmed,
	count(casos.downloaded_at) FILTER (
		WHERE class = 'PROBABLE'
	) AS new_probable,
	sum(count(casos.downloaded_at)) OVER (
		PARTITION BY grid.bulletin_date, fips
		ORDER BY grid.sample_date
	) AS cumulative_cases,
	sum(count(casos.downloaded_at) FILTER (
		WHERE class = 'CONFIRMADO'
	)) OVER (
		PARTITION BY grid.bulletin_date, fips
		ORDER BY grid.sample_date
	) AS cumulative_confirmed,
	sum(count(casos.downloaded_at) FILTER (
		WHERE class = 'PROBABLE'
	)) OVER (
		PARTITION BY grid.bulletin_date, fips
		ORDER BY grid.sample_date
	) AS cumulative_probable
FROM {{ ref('casos') }}
RIGHT OUTER JOIN grid
	ON grid.city = casos.city
	AND grid.sample_date = casos.sample_date
	AND grid.bulletin_date = date_add('day', -1, date(downloaded_at AT TIME ZONE 'America/Puerto_Rico'))
INNER JOIN {{ ref('municipal_population') }} popest
	USING (fips)
GROUP BY
    grid.bulletin_date,
    grid.sample_date,
    fips,
    display_name,
    pop2020,
    popest.region
ORDER BY
    grid.bulletin_date,
    grid.sample_date,
    display_name;
