--
-- Casos detectados inicialmente por prueba de antígeno
--
WITH bulletins AS (
	SELECT max(bulletin_date) AS max_bulletin_date
	FROM {{ ref('bioportal_curve') }} bioportal_curve
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
FROM {{ ref('bioportal_encounters_agg') }} bio
INNER JOIN bulletins
	ON bulletins.max_bulletin_date = bio.bulletin_date
ORDER BY bio.collected_date DESC;
