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
	(deaths.cumulative_deaths
		- lag(deaths.cumulative_deaths, 14) OVER (
			PARTITION BY deaths.bulletin_date
			ORDER BY deaths.datum_date
		)) / 14.0
		AS smoothed_deaths,
	(cases.cumulative_cases
		- lag(cases.cumulative_cases, 14) OVER (
			PARTITION BY cases.bulletin_date
			ORDER BY cases.collected_date
		)) / 14.0
		AS smoothed_cases,
	CAST(deaths.cumulative_deaths
		- lag(deaths.cumulative_deaths, 14) OVER (
			PARTITION BY deaths.bulletin_date
			ORDER BY deaths.datum_date
		) AS DOUBLE PRECISION)
		/ (cases.cumulative_cases
			- lag(cases.cumulative_cases, 14) OVER (
				PARTITION BY cases.bulletin_date
				ORDER BY cases.collected_date
			)) AS lagged_cfr
FROM {{ ref('bioportal_curve') }} cases
INNER JOIN deaths
	ON cases.bulletin_date = deaths.bulletin_date
	AND deaths.datum_date = date_add('day', 14, cases.collected_date)
ORDER BY cases.bulletin_date DESC, cases.collected_date DESC;
