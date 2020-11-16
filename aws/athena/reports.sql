--
-- This is just a collection of miscellaneous queries I often run
-- in an ad-hoc fashion against this database.
--

--
-- Compare the curves computed from the official report
-- and from Bioportal.
--
WITH bulletins AS (
	SELECT max(bulletin_date) AS max_bulletin_date
	FROM covid_pr_etl.bulletin_cases
)
SELECT
	max_bulletin_date "Datos hasta",
	bio.collected_date AS "Fecha muestra",
	COALESCE(bul.confirmed_cases, 0)
		+ COALESCE(bul.probable_cases, 0)
		AS "Casos (oficial)",
	bio.cases "Casos (Bioportal)",
	bio.cases
		- COALESCE(bul.confirmed_cases, 0)
		- COALESCE(bul.probable_cases, 0)
		AS "Exceso Bioportal"
FROM covid_pr_etl.bioportal_curve bio
INNER JOIN bulletins
	ON bulletins.max_bulletin_date = bio.bulletin_date
LEFT OUTER JOIN covid_pr_etl.bulletin_cases bul
	ON bul.bulletin_date = bio.bulletin_date
	AND bul.datum_date = bio.collected_date
WHERE bio.bulletin_date = DATE '2020-11-14'
ORDER BY bio.collected_date DESC;


--
-- Compare Bioportal molecular-only curve with molecular + antigens
--
WITH bulletins AS (
	SELECT max(bulletin_date) AS max_bulletin_date
	FROM covid_pr_etl.bioportal_collected_agg
)
SELECT
	max_bulletin_date "Datos hasta",
	molecular.collected_date "Fecha muestra",
	molecular.cases "Por molecular",
	(molecular.cumulative_cases - lag(molecular.cumulative_cases, 7) OVER (
		ORDER BY molecular.collected_date
	)) / 7.0 AS "Promedio (7 días)",
	antigens.cases - molecular.cases "Por antígenos",
	antigens.cases "Combinado",
	(antigens.cumulative_cases - lag(antigens.cumulative_cases, 7) OVER (
		ORDER BY molecular.collected_date
	)) / 7.0 AS "Promedio (7 días)"
FROM covid_pr_etl.bioportal_curve_molecular molecular
INNER JOIN bulletins
	ON bulletins.max_bulletin_date = molecular.bulletin_date
INNER JOIN covid_pr_etl.bioportal_curve antigens
	ON antigens.bulletin_date = molecular.bulletin_date
	AND antigens.collected_date = molecular.collected_date
ORDER BY "Fecha muestra" DESC;


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
WHERE test_type = 'Antigens'
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
WHERE test_type IN ('Molecular', 'Antigens')
AND collected_age <= 14
GROUP BY bulletins.since_bulletin_date, bulletins.until_bulletin_date, test_type
ORDER BY test_type;
