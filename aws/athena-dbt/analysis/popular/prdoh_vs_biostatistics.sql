SELECT
	bulletin_date "Datos hasta",
	collected_date "Muestras",
	encounters.cases AS "Mi análisis",
	covid19datos.confirmed_cases
		+ coalesce(covid19datos.probable_cases, 0)
		AS "Tablero Salud",
	covid19datos.confirmed_cases
		+ coalesce(covid19datos.probable_cases, 0)
		- encounters.cases
		AS "Salud - mío",
	100.0 * sum(covid19datos.confirmed_cases
				+ coalesce(covid19datos.probable_cases, 0)
				- encounters.cases) OVER seven
		/ sum(encounters.cases) OVER seven
		AS "% diferencia (7 días)"
FROM {{ ref('biostatistics_encounters_agg') }} encounters
INNER JOIN {{ ref('bulletin_cases') }} covid19datos
	USING (bulletin_date)
WHERE bulletin_date = (
	SELECT max(bulletin_date)
	FROM {{ ref('bulletin_cases') }}
)
AND covid19datos.datum_date = encounters.collected_date
WINDOW seven AS (
	PARTITION BY bulletin_date
	ORDER BY collected_date
	RANGE BETWEEN INTERVAL '6' DAY PRECEDING AND CURRENT ROW
)
ORDER BY bulletin_date DESC, collected_date DESC;
