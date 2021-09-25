--
-- This is just a collection of miscellaneous queries I often run
-- in an ad-hoc fashion against this database.
--


--
-- Recently received molecular test results.  Split into
-- "recent" and "late" tests by sample date.
--
SELECT
	test_type,
	bulletin_date,
	threshold.value late_threshold,
	sum(delta_tests) FILTER (WHERE collected_age <= threshold.value)
		AS recent_tests,
	sum(delta_positive_tests) FILTER (WHERE collected_age <= threshold.value)
		AS recent_positive_tests,
	sum(delta_tests) FILTER (WHERE collected_age > threshold.value)
		AS late_tests,
	sum(delta_positive_tests) FILTER (WHERE collected_age > threshold.value)
		AS late_positive_tests
FROM covid_pr_etl.bioportal_collected_agg
CROSS JOIN (VALUES (5)) AS threshold (value)
WHERE test_type IN ('Molecular', 'Antígeno')
GROUP BY test_type, bulletin_date, threshold.value
ORDER BY bulletin_date DESC, test_type DESC;


--
-- Datos básicos de hospitalizaciones
--
SELECT
	file_timestamp AS "Datos",
	date AS "Fecha",
	inpatient_beds_used_covid AS "Camas ocupadas por COVID",
	previous_day_admission_adult_covid_confirmed
		+ previous_day_admission_adult_covid_suspected
		+ previous_day_admission_pediatric_covid_confirmed
		+ previous_day_admission_pediatric_covid_suspected
		AS "Admisiones por COVID",
	staffed_icu_adult_patients_confirmed_and_suspected_covid
		AS "Camas UCI ocupadas por COVID",
	staffed_adult_icu_bed_occupancy AS "Camas UCI ocupadas (cualquier causa)",
	total_staffed_adult_icu_beds AS "Total camas UCI"
FROM covid_pr_etl.hhs_hospitals
ORDER BY date DESC;


--
-- Compare the curves computed from the official report
-- and from Bioportal.
--
WITH bulletins AS (
	SELECT max(bulletin_date) AS max_bulletin_date
	FROM covid19datos_v2_etl.bulletin_cases
)
SELECT
	max_bulletin_date "Datos",
	bio.collected_date AS "Muestras",
	COALESCE(bul.confirmed_cases, 0)
		+ COALESCE(bul.probable_cases, 0)
		AS "Casos (oficial)",
	((COALESCE(bul.cumulative_confirmed_cases, 0) + COALESCE(bul.cumulative_probable_cases , 0))
		- lag(COALESCE(bul.cumulative_confirmed_cases, 0) + COALESCE(bul.cumulative_probable_cases , 0), 7) OVER (
		ORDER BY bio.collected_date
	)) / 7.0 AS "Promedio (7 días)",
	bio.cases "Casos (Bioportal)",
	(bio.cumulative_cases - lag(bio.cumulative_cases, 7) OVER (
		ORDER BY bio.collected_date
	)) / 7.0 AS "Promedio (7 días)",
	bio.cases
		- COALESCE(bul.confirmed_cases, 0)
		- COALESCE(bul.probable_cases, 0)
		AS "Exceso Bioportal"
FROM covid_pr_etl.bioportal_encounters_agg bio
INNER JOIN bulletins
	ON bulletins.max_bulletin_date = bio.bulletin_date
INNER JOIN covid19datos_v2_etl.bulletin_cases bul
	ON bul.bulletin_date = bio.bulletin_date
	AND bul.datum_date = bio.collected_date
ORDER BY bio.collected_date DESC;


--
-- Bioportal curve.
--
WITH bulletins AS (
	SELECT max(bulletin_date) AS max_bulletin_date
	FROM covid_pr_etl.bioportal_curve
)
SELECT
	max_bulletin_date "Datos",
	bio.collected_date AS "Muestras",
	bio.encounters "Evaluados",
	(bio.cumulative_encounters - lag(bio.cumulative_encounters, 7) OVER (
		ORDER BY bio.collected_date
	)) / 7.0 AS "Promedio",
	bio.molecular "Molecular",
	(bio.cumulative_molecular - lag(bio.cumulative_molecular, 7) OVER (
		ORDER BY bio.collected_date
	)) / 7.0 AS "Promedio",
	bio.antigens "Antígeno",
	(bio.cumulative_antigens - lag(bio.cumulative_antigens, 7) OVER (
		ORDER BY bio.collected_date
	)) / 7.0 AS "Promedio",
	bio.cases "Casos",
	(bio.cumulative_cases - lag(bio.cumulative_cases, 7) OVER (
		ORDER BY bio.collected_date
	)) / 7.0 AS "Promedio"
FROM covid_pr_etl.bioportal_encounters_agg bio
INNER JOIN bulletins
	ON bulletins.max_bulletin_date = bio.bulletin_date
ORDER BY bio.collected_date DESC;


--
-- Casos detectados inicialmente por prueba de antígeno
--
WITH bulletins AS (
	SELECT max(bulletin_date) AS max_bulletin_date
	FROM covid_pr_etl.bioportal_curve
)
SELECT
	max_bulletin_date "Datos",
	bio.collected_date AS "Muestras",
	(bio.cumulative_cases - lag(bio.cumulative_cases, 7) OVER (
		ORDER BY bio.collected_date
	)) / 7.0 AS "Casos",
	(bio.cumulative_antigens_cases - lag(bio.cumulative_antigens_cases, 7) OVER (
		ORDER BY bio.collected_date
	)) / 7.0 AS "Por antígeno",
	100.0 * (bio.cumulative_antigens_cases - lag(bio.cumulative_antigens_cases, 7) OVER (
		ORDER BY bio.collected_date
	)) / (bio.cumulative_cases - lag(bio.cumulative_cases, 7) OVER (
		ORDER BY bio.collected_date
	)) "Porcentaje"
FROM covid_pr_etl.bioportal_encounters_agg bio
INNER JOIN bulletins
	ON bulletins.max_bulletin_date = bio.bulletin_date
ORDER BY bio.collected_date DESC;


--
-- Recent history of antigen test volume and positive rates
--
WITH bulletins AS (
	SELECT max(bulletin_date) AS max_bulletin_date
	FROM covid_pr_etl.bioportal_collected_agg
)
SELECT
	max_bulletin_date "Datos hasta",
	collected_date "Fecha de muestra",
	tests "Pruebas antígeno",
	positive_tests "Positivas",
	100.0 * (cumulative_positives - lag(cumulative_positives, 7) OVER (
		PARTITION BY test_type, bulletin_date
		ORDER BY collected_date
	)) / (cumulative_tests - lag(cumulative_tests, 7) OVER (
			PARTITION BY test_type, bulletin_date
			ORDER BY collected_date
		 )) AS "% positivas (7 días)",
 	delta_tests "Recién recibidas",
-- 	delta_positive_tests "Positivas",
	100.0 * NULLIF(delta_positive_tests, 0)
		/ delta_tests AS "% positivas"
FROM covid_pr_etl.bioportal_collected_agg bca
INNER JOIN bulletins
	ON bulletins.max_bulletin_date = bca.bulletin_date
WHERE test_type = 'Antígeno'
ORDER BY collected_date DESC;


--
-- Lag of recently received molecular and antigen test results.
-- Excludes very old samples because they're outside of the
-- "happy path."
--
WITH bulletins AS (
	SELECT
		max(bulletin_date) AS until_bulletin_date,
		date_add('day', -7, max(bulletin_date))
			AS since_bulletin_date
	FROM covid_pr_etl.bioportal_collected_agg
)
SELECT
	bulletins.since_bulletin_date "Desde",
	bulletins.until_bulletin_date "Hasta",
	test_type "Tipo de prueba",
	sum(delta_positive_tests * collected_age)
		/ cast(sum(delta_positive_tests) AS DOUBLE PRECISION)
		AS "Rezago (positivas)",
	sum(delta_tests * collected_age)
		/ cast(sum(delta_tests) AS DOUBLE PRECISION)
		AS "Rezago (todas)"
FROM covid_pr_etl.bioportal_collected_agg bca
INNER JOIN bulletins
	ON bulletins.since_bulletin_date < bca.bulletin_date
	AND bca.bulletin_date <= bulletins.until_bulletin_date
WHERE test_type IN ('Molecular', 'Antígeno')
AND collected_age <= 14
GROUP BY bulletins.since_bulletin_date, bulletins.until_bulletin_date, test_type
ORDER BY test_type;


--
-- HHS Community Profile Report test data for Puerto Rico,
-- which has often been absurdly wrong.
--
SELECT
	date AS "Fecha reporte federal",
	sum(total_tests_last_7_days) / 7.0
		AS "Pruebas diarias",
	sum(total_positive_tests_last_7_days) / 7.0
		AS "Positivas diarias",
	100.0 * sum(total_positive_tests_last_7_days)
		/ sum(total_tests_last_7_days)
		AS "Dizque positividad"
FROM covid_hhs_sources.community_profile_report_municipios
GROUP BY date
ORDER BY date ASC;


--
-- Puerto Rico PCR test volume and positive rate according to the HHS's
-- very unreliable Diagnostic Lab Testing dataset.
--
WITH bulletins AS (
	SELECT max(bulletin_date) AS max_bulletin_date
	FROM covid_hhs_sources.diagnostic_lab_testing_PR_daily
)
SELECT
	bulletin_date "Datos hasta",
	date "Fecha pruebas",
	tests "Pruebas",
	(cumulative_tests - lag(cumulative_tests, 7) OVER (
		PARTITION BY bulletin_date
		ORDER BY date
	)) / 7.0 AS "Promedio 7 días",
	positive "Positivas",
	100.0 * (cumulative_positive - lag(cumulative_positive, 7) OVER (
		PARTITION BY bulletin_date
		ORDER BY date
	)) / (cumulative_tests - lag(cumulative_tests, 7) OVER (
		PARTITION BY bulletin_date
		ORDER BY date
	)) AS "Positividad (7 días)"
FROM covid_hhs_sources.diagnostic_lab_testing_PR_daily
INNER JOIN bulletins
    ON bulletin_date = max_bulletin_date
ORDER BY date DESC;


WITH hhs_date AS (
	SELECT max(bulletin_date) AS max_bulletin_date
	FROM covid_hhs_sources.diagnostic_lab_testing_PR_daily
), prdoh_date AS (
	SELECT max(bulletin_date) AS max_bulletin_date
	FROM covid_pr_etl.bioportal_curve
), agg AS (
	SELECT
		bio.bulletin_date "Datos Salud",
		hhs.bulletin_date "Datos HHS",
		bio.reported_date AS "Fecha pruebas",
		bio.tests "Salud",
		(bio.cumulative_tests - lag(bio.cumulative_tests, 7) OVER (
			ORDER BY bio.reported_date
		)) / 7.0 AS "Promedio Salud",
		hhs.tests "Federales",
		(hhs.cumulative_tests - lag(hhs.cumulative_tests, 7) OVER (
			ORDER BY date
		)) / 7.0 AS "Promedio federales"
	FROM covid_pr_etl.bioportal_reported_agg bio
	INNER JOIN prdoh_date
		ON prdoh_date.max_bulletin_date = bio.bulletin_date
	INNER JOIN covid_hhs_sources.diagnostic_lab_testing_PR_daily hhs
		ON hhs.date = bio.reported_date
	INNER JOIN hhs_date
		ON hhs_date.max_bulletin_date = hhs.bulletin_date
	WHERE bio.test_type = 'Molecular'
)
SELECT
	*,
	"Promedio Salud" - "Promedio federales"
		AS "Promedio Salud - Promedio federales"
FROM agg
ORDER BY "Fecha pruebas" DESC;


--
-- Puerto Rico PCR test volume and positive rate according to the HHS's
-- very unreliable Community Profile Report.
--
SELECT
	date,
	sum(total_tests_last_7_days) / 7.0 AS "Pruebas diarias",
	sum(total_positive_tests_last_7_days) / 7.0 AS "Positivas diarias",
	100.0 * sum(total_positive_tests_last_7_days)
		/ sum(total_tests_last_7_days)
		AS "Positividad"
FROM covid_hhs_sources.community_profile_report_municipios
GROUP BY date
ORDER BY date DESC;


--
-- Compare the PREIS and Tiberius numbers to watch for
-- PRDoH shenanigans
--
SELECT
	downloaded_at AT TIME ZONE 'America/Puerto_Rico'
		AS "Hora de descarga",
	registradas,
	registradas1,
	registradas2,
	tiberius_total,
	tiberius_1dosis,
	tiberius_2dosis
FROM covid19datos_sources.vacunaciones_flat;


--
-- Doubling times, using 7- and 14-day windows
--
WITH bulletins AS (
	SELECT max(bulletin_date) AS max_bulletin_date
	FROM covid_pr_etl.bioportal_encounters_agg
)
SELECT
	bulletin_date,
	collected_date,
	(cumulative_cases - lag(cumulative_cases, 7) OVER (
		PARTITION BY bulletin_date
		ORDER BY collected_date
	)) cases_7,
	7 / (
		log2(cumulative_cases - lag(cumulative_cases, 7) OVER (
			PARTITION BY bulletin_date
			ORDER BY collected_date
		)) - log2(lag(cumulative_cases, 7) OVER (
			PARTITION BY bulletin_date
			ORDER BY collected_date
		) - lag(cumulative_cases, 14) OVER (
			PARTITION BY bulletin_date
			ORDER BY collected_date
		))
	) doubling_days_7,
	(cumulative_cases - lag(cumulative_cases, 14) OVER (
		PARTITION BY bulletin_date
		ORDER BY collected_date
	)) cases_14,
	14 / (
		log2(cumulative_cases - lag(cumulative_cases, 14) OVER (
			PARTITION BY bulletin_date
			ORDER BY collected_date
		)) - log2(lag(cumulative_cases, 14) OVER (
			PARTITION BY bulletin_date
			ORDER BY collected_date
		) - lag(cumulative_cases, 28) OVER (
			PARTITION BY bulletin_date
			ORDER BY collected_date
		))
	) doubling_days_14
FROM covid_pr_etl.bioportal_encounters_agg bio
INNER JOIN bulletins
	ON bulletins.max_bulletin_date = bio.bulletin_date
ORDER BY bulletin_date, collected_date DESC;


--
-- Pediatric tests, cases and deaths
--
WITH bulletins AS (
	SELECT max(bulletin_date) AS max_bulletin_date
	FROM covid_pr_etl.bioportal_encounters_cube
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
FROM covid_pr_etl.bioportal_acs_age_curve encounters
INNER JOIN bulletins
	ON bulletins.max_bulletin_date = encounters.bulletin_date
WHERE age_lt <= 20
ORDER BY bulletin_date, collected_date DESC, age_lt;