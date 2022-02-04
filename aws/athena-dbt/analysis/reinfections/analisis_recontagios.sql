--
-- Análisis de recontagios.  Comparación de conteo oficial con
-- mi análisis que toma en cuenta posibles recontagios.
--
WITH bulletins AS (
	SELECT max(bulletin_date) AS bulletin_date
	FROM covid19_puerto_rico_model.bioportal_curve
)
SELECT
	bulletins.bulletin_date "Datos",
	collected_date AS "Muestras",
	bul.confirmed_cases + COALESCE(bul.probable_cases, 0)
		AS "Oficial",
	((bul.cumulative_confirmed_cases + COALESCE(bul.cumulative_probable_cases, 0))
		- lag(bul.cumulative_confirmed_cases + COALESCE(bul.cumulative_probable_cases , 0), 7) OVER (
		ORDER BY bio.collected_date
	)) / 7.0 AS "Promedio",
	bio.cases "Bioportal",
	(bio.cumulative_cases - lag(bio.cumulative_cases, 7) OVER (
		ORDER BY bio.collected_date
	)) / 7.0 AS "Promedio",
	first_infections "Primeros",
	(cumulative_first_infections - lag(cumulative_first_infections, 7) OVER (
		ORDER BY bio.collected_date
	)) / 7.0 AS "Promedio",
	possible_reinfections "Recontagios",
	(cumulative_possible_reinfections - lag(cumulative_possible_reinfections, 7) OVER (
		ORDER BY collected_date
	)) / 7.0 AS "Promedio"
FROM {{ ref('bioportal_encounters_agg') }} bio
INNER JOIN bulletins
	ON bulletins.bulletin_date = bio.bulletin_date
LEFT OUTER JOIN {{ ref('bulletin_cases') }} bul
	ON bul.bulletin_date = bulletins.bulletin_date
	AND bul.datum_date = bio.collected_date
ORDER BY collected_date DESC;
