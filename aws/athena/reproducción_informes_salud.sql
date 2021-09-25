--
-- Reproducir (más or menos) la carátula del viejo informe de casos
--
SELECT
	bulletin_date "Fecha actualización de datos",
	sum(delta_confirmed_cases) FILTER (
		WHERE date_add('day', -16, bulletin_date) <= datum_date
	) AS "Confirmados nuevos",
	sum(delta_confirmed_cases) FILTER (
		WHERE date_add('day', -16, bulletin_date) > datum_date
	) AS "Ajustes",
	sum(confirmed_cases) AS "Acumulados",
	sum(delta_probable_cases) FILTER (
		WHERE date_add('day', -16, bulletin_date) <= datum_date
	) "Probables nuevos",
	sum(delta_probable_cases) FILTER (
		WHERE date_add('day', -16, bulletin_date) > datum_date
	) AS "Ajustes",
	sum(probable_cases) AS "Acumulados"
FROM covid19datos_v2_etl.bulletin_cases
GROUP BY bulletin_date
ORDER BY bulletin_date DESC;


--
-- Reproducir las tablas de casos confirmados y probables por fecha de muestra
--
WITH bulletins AS (
	SELECT max(bulletin_date) AS bulletin_date
	FROM covid19datos_v2_etl.bulletin_cases
)
SELECT
	bulletin_date "Fecha de actualización de datos",
	datum_date "Fecha de muestra",
	confirmed_cases "Casos confirmados",
	cumulative_confirmed_cases "Acumulados",
	probable_cases "Casos probables",
	cumulative_probable_cases "Casos probables"
FROM covid19datos_v2_etl.bulletin_cases
INNER JOIN bulletins USING (bulletin_date)
ORDER BY datum_date DESC;


--
-- Aproximación del informe de transmisión comunitaria. Requiere datos
-- municipales de pruebas de Bioportal.
--
WITH bulletins AS (
	SELECT max(bulletin_date) AS bulletin_date
	FROM covid19datos_v2_etl.bulletin_cases
), casos AS (
	SELECT
		bulletin_date,
		municipality,
		pop2020,
		sum(new_cases) new_cases,
		1e5 * sum(new_cases) / pop2020 itc1
	FROM covid19datos_v2_etl.cases_municipal_agg
	INNER JOIN bulletins USING (bulletin_date)
	WHERE date_add('day', -10, bulletin_date) <= sample_date
	AND sample_date <= date_add('day', -4, bulletin_date)
	GROUP BY bulletin_date, municipality, pop2020
), pruebas AS (
	SELECT
		bulletin_date,
		municipality,
		sum(specimens) AS specimens,
		sum(positives) AS positives,
		100.0 * sum(positives) / sum(specimens) AS itc2
	FROM covid_pr_etl.municipal_tests_collected_agg tests
	INNER JOIN bulletins USING (bulletin_date)
	INNER JOIN covid_pr_sources.municipal_population pop
		ON pop.name = tests.municipality
	WHERE date_add('day', -10, bulletin_date) <= collected_date
	AND collected_date <= date_add('day', -4, bulletin_date)
	AND test_type = 'Molecular'
	GROUP BY bulletin_date, municipality
), niveles AS (
	SELECT
		bulletin_date,
		municipality,
		CASE
			WHEN itc1 < 10 THEN 0
			WHEN itc1 < 50 THEN 1
			WHEN itc1 < 100 THEN 2
			ELSE 3
		END AS itc1_nivel,
		CASE
			WHEN itc2 < 5.0 THEN 0
			WHEN itc2 < 8.0 THEN 1
			WHEN itc2 < 10.0 THEN 2
			ELSE 3
		END AS itc2_nivel
	FROM casos
	INNER JOIN pruebas USING (bulletin_date, municipality)
)
SELECT
	bulletin_date "Datos hasta",
	date_add('day', -13, bulletin_date) "Muestras desde",
	date_add('day', -7, bulletin_date) "Muestras hasta",
	municipality "Municipio",
	new_cases "Casos",
	itc1 "Tasa de casos (ITC1)",
	itc2 "Positividad (ITC2)",
	CASE greatest(itc1_nivel, itc2_nivel)
		WHEN 0 THEN 'Azul'
		WHEN 1 THEN 'Amarillo'
		WHEN 2 THEN 'Anaranjado'
		WHEN 3 THEN 'Rojo'
	END AS "Color"
FROM casos
INNER JOIN pruebas USING (bulletin_date, municipality)
INNER JOIN niveles USING (bulletin_date, municipality)
ORDER BY
	bulletin_date DESC,
	greatest(itc1_nivel, itc2_nivel) DESC,
	itc1 DESC,
	itc2 DESC,
	municipality;
