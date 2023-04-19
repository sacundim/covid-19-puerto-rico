--
-- Pediatric tests, cases and deaths
--
WITH bulletins AS (
	SELECT max(bulletin_date) AS bulletin_date
	FROM {{ ref('bioportal_encounters_cube') }}
)
SELECT
	bulletin_date,
	collected_date,
	COALESCE(age_lt <= 20, FALSE) under_20,
	sum(population) population_2019,
	sum(encounters) people_tested,
	sum(sum(encounters)) OVER (
		PARTITION BY bulletin_date, COALESCE(age_lt <= 20, FALSE)
		ORDER BY collected_date
	) cumulative_people_tested,
	sum(cases) cases,
	sum(sum(cases)) OVER (
		PARTITION BY bulletin_date, COALESCE(age_lt <= 20, FALSE)
		ORDER BY collected_date
	) cumulative_cases,
	sum(deaths) deaths,
	sum(sum(deaths)) OVER (
		PARTITION BY bulletin_date, COALESCE(age_lt <= 20, FALSE)
		ORDER BY collected_date
	) cumulative_deaths,
	CASE COALESCE(age_lt <= 20, FALSE)
		WHEN TRUE
		THEN max(hospitalizations.camas_ped_covid)
		ELSE max(hospitalizations.camas_adultos_covid)
	END AS camas_covid,
	CASE COALESCE(age_lt <= 20, FALSE)
		WHEN TRUE
		THEN max(hospitalizations.camas_picu_covid)
		ELSE max(hospitalizations.camas_aicu_covid)
	END AS camas_icu_covid,
	CASE COALESCE(age_lt <= 20, FALSE)
		WHEN TRUE
		THEN max(hospitalizations.admission_pediatric_covid)
		ELSE max(hospitalizations.admission_adult_covid)
	END AS admission_covid
FROM {{ ref('bioportal_acs_age_curve') }} encounters
INNER JOIN bulletins
	USING (bulletin_date)
INNER JOIN {{ ref('hospitalizations') }} hospitalizations
	USING (bulletin_date)
WHERE hospitalizations.date = encounters.collected_date
GROUP BY bulletin_date, collected_date, COALESCE(age_lt <= 20, FALSE)
ORDER BY bulletin_date, collected_date DESC;


--
-- Pediatric tests, cases and deaths
--
WITH bulletins AS (
	SELECT max(bulletin_date) AS max_bulletin_date
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
INNER JOIN bulletins
	ON bulletins.max_bulletin_date = encounters.bulletin_date
WHERE age_lt <= 20
ORDER BY bulletin_date, collected_date DESC, age_lt;