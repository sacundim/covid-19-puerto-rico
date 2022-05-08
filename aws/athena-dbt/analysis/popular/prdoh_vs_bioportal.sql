--
-- Compare the curves computed from the official report
-- and from Bioportal.
--
WITH bulletins AS (
	SELECT max(bulletin_date) AS max_bulletin_date
	FROM {{ ref('bulletin_cases') }}
)
SELECT
	bio.bulletin_date "Datos",
	bio.collected_date AS "Muestras",
	bul.confirmed_cases + COALESCE(bul.probable_cases, 0)
		AS "Casos (oficial)",
	sum(bul.confirmed_cases + COALESCE(bul.probable_cases , 0)) OVER (
    	PARTITION BY bio.bulletin_date
		ORDER BY bio.collected_date
		ROWS 6 PRECEDING
	) / 7.0 AS "Promedio (7 días)",
	bio.cases "Casos (Bioportal)",
	sum(bio.cases) OVER (
    	PARTITION BY bio.bulletin_date
		ORDER BY bio.collected_date
		ROWS 6 PRECEDING
	)) / 7.0 AS "Promedio (7 días)",
	bio.cases
		- COALESCE(bul.confirmed_cases, 0)
		- COALESCE(bul.probable_cases, 0)
		AS "Exceso Bioportal"
FROM {{ ref('bioportal_encounters_agg') }} bio
INNER JOIN bulletins
	ON bulletins.max_bulletin_date = bio.bulletin_date
LEFT OUTER JOIN {{ ref('bulletin_cases') }} bul
	ON bul.bulletin_date = bio.bulletin_date
	AND bul.datum_date = bio.collected_date
ORDER BY bio.collected_date DESC;
