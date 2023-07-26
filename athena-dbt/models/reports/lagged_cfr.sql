--
-- Lagged case fatality rate, comparing 14-day average of deaths
-- with 14-day average of cases 14 days earlier.
--

WITH deaths AS (
	SELECT
		bulletin_date,
		datum_date,
		deaths,
		sum(deaths) OVER (
			PARTITION BY bulletin_date
			ORDER BY datum_date
		) AS cumulative_deaths
	FROM {{ ref('bulletin_cases') }}
)
SELECT
	cases.bulletin_date,
	cases.collected_date,
	deaths.datum_date death_date,
	sum(deaths.deaths) OVER (
		PARTITION BY deaths.bulletin_date
		ORDER BY deaths.datum_date
		ROWS 13 PRECEDING
	) / 14.0 AS smoothed_deaths,
	sum(cases.cases) OVER (
		PARTITION BY deaths.bulletin_date
		ORDER BY deaths.datum_date
		ROWS 13 PRECEDING
	) / 14.0 AS smoothed_cases,
	CAST(sum(deaths.deaths) OVER (
			PARTITION BY deaths.bulletin_date
			ORDER BY deaths.datum_date
			ROWS 13 PRECEDING
		) AS DOUBLE PRECISION)
		/ sum(cases.cases) OVER (
			PARTITION BY deaths.bulletin_date
			ORDER BY deaths.datum_date
			ROWS 13 PRECEDING
		) AS lagged_cfr
FROM {{ ref('biostatistics_curve') }} cases
INNER JOIN deaths
	ON cases.bulletin_date = deaths.bulletin_date
	AND deaths.datum_date = date_add('day', 14, cases.collected_date)
WHERE cases.collected_date >= DATE '2020-03-13'
ORDER BY cases.bulletin_date, cases.collected_date;
