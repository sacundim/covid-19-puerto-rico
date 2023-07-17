WITH bulletins AS (
	SELECT max(bulletin_date) AS bulletin_date
	FROM covid19_puerto_rico_model.municipal_tests_collected_agg
), baseline AS (
	SELECT
		bulletin_date,
		municipality,
		min(collected_date) since,
		max(collected_date) until,
		21 AS days_coverage,
		sum(specimens) AS specimens
	FROM {{ ref('municipal_tests_collected_agg') }} tests
	INNER JOIN bulletins USING (bulletin_date)
	WHERE date_add('day', -21, DATE '2022-09-17') < collected_date
	AND collected_date <= DATE '2022-09-17'
	AND test_type IN ('Molecular', 'Antígeno')
	GROUP BY bulletin_date, municipality
), fiona AS (
	SELECT
		bulletin_date,
		municipality,
		min(collected_date) since,
		max(collected_date) until,
		7 AS days_coverage,
		sum(specimens) AS specimens
	FROM {{ ref('municipal_tests_collected_agg') }} tests
	INNER JOIN bulletins USING (bulletin_date)
	WHERE DATE '2022-09-17' < collected_date
	AND collected_date <= date_add('day', 7, DATE '2022-09-17')
	AND test_type IN ('Molecular', 'Antígeno')
	GROUP BY bulletin_date, municipality
)
SELECT
	bulletin_date "Datos",
	region "Región",
	min(baseline.since) "Muestras desde",
	max(baseline.until) "Hasta",
	1e5 * sum(baseline.specimens)
		/ max(baseline.days_coverage)
		/ sum(muni.popest2019)
		AS "Pruebas/día/100k",
	min(fiona.since) "Muestras desde",
	max(fiona.until) "Hasta",
	1e5 * sum(fiona.specimens)
		/ max(fiona.days_coverage)
		/ sum(muni.popest2019)
		AS "Pruebas/día/100k",
	100.0 * (sum(fiona.specimens) / max(fiona.days_coverage) - sum(baseline.specimens) / max(baseline.days_coverage))
			/ (sum(baseline.specimens) / max(baseline.days_coverage))
		AS "% cambio"
FROM baseline
INNER JOIN fiona
	USING (bulletin_date, municipality)
INNER JOIN {{ ref('cases_municipal_agg') }} muni
	ON muni.name = municipality
GROUP BY bulletin_date, region
ORDER BY "% cambio";