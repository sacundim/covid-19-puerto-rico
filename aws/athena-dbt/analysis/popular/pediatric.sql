--
-- Pediatric tests, cases and deaths
--
WITH bulletins AS (
	SELECT max(bulletin_date) AS max_bulletin_date
	FROM {{ ref('bioportal_encounters_cube') }}
)
SELECT
	bulletin_date,
	collected_date,
	age_gte,
	age_lt,
	population,
	encounters,
	sum(encounters) OVER (
		PARTITION BY bulletin_date, age_gte
		ORDER BY collected_date
	) cumulative_encounters,
	cases,
	sum(cases) OVER (
		PARTITION BY bulletin_date, age_gte
		ORDER BY collected_date
	) cumulative_cases,
	deaths,
	sum(deaths) OVER (
		PARTITION BY bulletin_date, age_gte
		ORDER BY collected_date
	) cumulative_deaths
FROM {{ ref('bioportal_acs_age_curve') }} encounters
INNER JOIN bulletins
	ON bulletins.max_bulletin_date = encounters.bulletin_date
WHERE age_lt <= 20
ORDER BY bulletin_date, collected_date DESC, age_lt;