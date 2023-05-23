--
-- Bioportal curve.
--
WITH bulletins AS (
	SELECT max(bulletin_date) AS max_bulletin_date
	FROM {{ ref('bioportal_encounters_agg') }}
)
SELECT
	bio.bulletin_date "Datos",
	bio.collected_date AS "Muestras",
	bio.encounters "Evaluados",
	sum(encounters) OVER (
	    PARTITION BY bio.bulletin_date
	    ORDER BY bio.collected_date
	    ROWS 6 PRECEDING
	) / 7.0 AS "Promedio",
	bio.molecular "Molecular",
	sum(molecular) OVER (
	    PARTITION BY bio.bulletin_date
	    ORDER BY bio.collected_date
	    ROWS 6 PRECEDING
	) / 7.0 AS "Promedio",
	bio.antigens "Antígeno",
	sum(antigens) OVER (
	    PARTITION BY bio.bulletin_date
	    ORDER BY bio.collected_date
	    ROWS 6 PRECEDING
	) / 7.0 AS "Promedio",
	bio.cases "Casos",
	sum(cases) OVER (
	    PARTITION BY bio.bulletin_date
	    ORDER BY bio.collected_date
	    ROWS 6 PRECEDING
	) / 7.0 AS "Promedio"
FROM {{ ref('bioportal_encounters_agg') }} bio
INNER JOIN bulletins
	ON bulletins.max_bulletin_date = bio.bulletin_date
ORDER BY bio.collected_date DESC;
