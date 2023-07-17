--
-- Aproximación del informe de transmisión comunitaria. Requiere datos
-- municipales de pruebas de Bioestadísticas.
--
WITH bulletins AS (
	SELECT max(bulletin_date) AS bulletin_date
	FROM {{ ref('bulletin_cases') }}
), casos AS (
	SELECT
		bulletin_date,
		municipality,
		pop2020,
		sum(new_cases) new_cases,
		1e5 * sum(new_cases) / pop2020 itc1
	FROM {{ ref('cases_municipal_agg') }}
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
	FROM {{ ref('municipal_tests_collected_agg') }} tests
	INNER JOIN bulletins USING (bulletin_date)
	INNER JOIN {{ ref('municipal_population') }} pop
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
