--
-- Pediatric tests, cases and deaths
--
WITH bulletins AS (
	SELECT max(bulletin_date) AS bulletin_date
	FROM {{ ref('bioportal_encounters_cube') }}
)
SELECT
	bulletin_date datos_hasta,
	collected_date fecha_muestra,
	COALESCE(age_lt <= 20, FALSE) menores,
	sum(population) poblacion_2019,
	sum(encounters) pruebas,
	sum(sum(encounters)) OVER (
		PARTITION BY bulletin_date, COALESCE(age_lt <= 20, FALSE)
		ORDER BY collected_date
	) pruebas_acumuladas,
	sum(antigens) antigenos,
	sum(sum(antigens)) OVER (
		PARTITION BY bulletin_date, COALESCE(age_lt <= 20, FALSE)
		ORDER BY collected_date
	) antigenos_acumuladas,
	sum(molecular) moleculares,
	sum(sum(molecular)) OVER (
		PARTITION BY bulletin_date, COALESCE(age_lt <= 20, FALSE)
		ORDER BY collected_date
	) moleculares_acumuladas,
	sum(cases) casos,
	sum(sum(cases)) OVER (
		PARTITION BY bulletin_date, COALESCE(age_lt <= 20, FALSE)
		ORDER BY collected_date
	) casos_acumulados,
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
	END AS admisiones_covid_hhs
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
ORDER BY bulletin_date, collected_date, age_lt;