--
-- Casos detectados inicialmente por prueba de antígeno
--
WITH bulletins AS (
	SELECT max(bulletin_date) AS max_bulletin_date
	FROM {{ ref('bioportal_encounters_agg') }} bioportal_curve
)
SELECT
	max_bulletin_date "Datos",
	bio.collected_date AS "Muestras",
    sum(cases) OVER (
	    PARTITION BY bio.bulletin_date
	    ORDER BY bio.collected_date
	    ROWS 6 PRECEDING
	) / 7.0 AS "Casos",
    sum(antigens_cases) OVER (
	    PARTITION BY bio.bulletin_date
	    ORDER BY bio.collected_date
	    ROWS 6 PRECEDING
	) / 7.0 AS "Por antígeno",
	100.0 * sum(bio.antigens_cases) OVER (
	    PARTITION BY bio.bulletin_date
	    ORDER BY bio.collected_date
	    ROWS 6 PRECEDING
	) / sum(bio.cases) OVER (
	    PARTITION BY bio.bulletin_date
	    ORDER BY bio.collected_date
	    ROWS 6 PRECEDING
	) "Porcentaje"
FROM {{ ref('bioportal_encounters_agg') }} bio
INNER JOIN bulletins
	ON bulletins.max_bulletin_date = bio.bulletin_date
ORDER BY bio.collected_date DESC;
