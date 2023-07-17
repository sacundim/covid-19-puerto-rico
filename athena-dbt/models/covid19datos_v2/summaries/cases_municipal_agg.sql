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
	    downloaded_at,
		bulletin_date,
		date(date_column) AS sample_date,
		city,
		display_name,
		fips
	FROM {{ ref('covid19datos_v2_casos_city_names') }}
	CROSS JOIN (
		VALUES (SEQUENCE(DATE '{{ var("first_sample_date") }}', DATE '{{ var("end_date") }}', INTERVAL '1' DAY))
	) AS date_array (date_array)
	CROSS JOIN UNNEST(date_array) AS t2(date_column)
	INNER JOIN downloads
		ON CAST(date_column AS DATE) < bulletin_date
), counts AS (
    SELECT
        grid.bulletin_date,
        grid.sample_date,
        popest.region,
        display_name municipality,
        fips,
        popest2019,
        pop2020,
        count(casos.downloaded_at) new_cases,
        count(casos.downloaded_at) FILTER (
            WHERE class = 'CONFIRMADO'
        ) AS new_confirmed,
        count(casos.downloaded_at) FILTER (
            WHERE class = 'PROBABLE'
        ) AS new_probable
    FROM {{ ref('casos') }}
    RIGHT OUTER JOIN grid
        ON grid.city = casos.city
        AND grid.sample_date = casos.sample_date
        AND grid.downloaded_at = casos.downloaded_at
    INNER JOIN {{ ref('municipal_population') }} popest
        USING (fips)
    GROUP BY
        grid.bulletin_date,
        grid.sample_date,
        fips,
        display_name,
        popest2019,
        pop2020,
        popest.region
)
SELECT
    *,
	sum(new_cases) OVER (
		PARTITION BY bulletin_date, fips
		ORDER BY sample_date
	) AS cumulative_cases,
	sum(new_confirmed) OVER (
		PARTITION BY bulletin_date, fips
		ORDER BY sample_date
	) AS cumulative_confirmed,
	sum(new_probable) OVER (
		PARTITION BY bulletin_date, fips
		ORDER BY sample_date
	) AS cumulative_probable,
	new_cases - lag(new_cases, 1, 0) OVER (
	    PARTITION BY sample_date, fips
	    ORDER BY bulletin_date
	) AS delta_cases,
	new_confirmed - lag(new_confirmed, 1, 0) OVER (
        PARTITION BY sample_date, fips
        ORDER BY bulletin_date
	) AS delta_confirmed,
	new_probable - lag(new_probable, 1, 0) OVER (
        PARTITION BY sample_date, fips
        ORDER BY bulletin_date
	) AS delta_probable
FROM counts
ORDER BY
    bulletin_date,
    sample_date,
    municipality;
