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
FROM {{ ref('bulletin_cases') }}
GROUP BY bulletin_date
ORDER BY bulletin_date DESC;