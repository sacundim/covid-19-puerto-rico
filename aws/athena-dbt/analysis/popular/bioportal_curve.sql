--
-- Bioportal curve.
--
WITH bulletins AS (
	SELECT max(bulletin_date) AS max_bulletin_date
	FROM {{ ref('bioportal_curve') }}
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
FROM {{ ref('bioportal_encounters_agg') }} bio
INNER JOIN bulletins
	ON bulletins.max_bulletin_date = bio.bulletin_date
ORDER BY bio.collected_date DESC;
