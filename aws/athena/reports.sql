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
-- Bioportal curve.
--
WITH bulletins AS (
	SELECT max(bulletin_date) AS max_bulletin_date
	FROM covid_pr_etl.bioportal_curve
)
SELECT
	max_bulletin_date "Datos",
	bio.datum_date AS "Muestras",
	bio.pcr "Moleculares",
	(bio.cumulative_pcr - lag(bio.cumulative_pcr, 7) OVER (
		ORDER BY bio.datum_date
	)) / 7.0 AS "Promedio 7 días",
	bio.antigens "Antígeno",
	(bio.cumulative_antigens - lag(bio.cumulative_antigens, 7) OVER (
		ORDER BY bio.datum_date
	)) / 7.0 AS "Promedio 7 días",
	bio.cases "Casos (Bioportal)",
	(bio.cumulative_cases - lag(bio.cumulative_cases, 7) OVER (
		ORDER BY bio.datum_date
	)) / 7.0 AS "Promedio 7 días"
FROM covid_pr_etl.recent_daily_cases bio
INNER JOIN bulletins
	ON bulletins.max_bulletin_date = bio.bulletin_date
ORDER BY bio.datum_date DESC;


--
-- Datos básicos de hospitalizaciones
--
SELECT
	file_timestamp AS "Datos",
	date AS "Fecha",
	inpatient_beds_used_covid AS "Camas ocupadas por COVID",
	staffed_adult_icu_bed_occupancy AS "Camas UCI ocupadas (cualquier causa)"
FROM covid_pr_etl.hhs_hospitals
ORDER BY date DESC;


--
-- Compare the curves computed from the official report
-- and from Bioportal.
--
WITH bulletins AS (
	SELECT max(bulletin_date) AS max_bulletin_date
	FROM covid_pr_etl.bulletin_cases
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
FROM covid_pr_etl.bioportal_curve bio
INNER JOIN bulletins
	ON bulletins.max_bulletin_date = bio.bulletin_date
INNER JOIN covid_pr_etl.bulletin_cases bul
	ON bul.bulletin_date = bio.bulletin_date
	AND bul.datum_date = bio.collected_date
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