WITH bulletins AS (
	SELECT max(bulletin_date) AS bulletin_date
	FROM {{ ref('biostatistics_encounters_agg') }}
), rates AS (
	SELECT
		bulletin_date,
		collected_date,
		sum(cases) OVER (
		    PARTITION BY bulletin_date
			ORDER BY collected_date
			ROWS 6 PRECEDING
		) / 7.0 AS cases,
		sum(positive_antigens) OVER (
		    PARTITION BY bulletin_date
			ORDER BY collected_date
			ROWS 6 PRECEDING
		) AS positive_antigens,
		sum(antigens) OVER (
		    PARTITION BY bulletin_date
			ORDER BY collected_date
			ROWS 6 PRECEDING
		) AS antigens,
		sum(positive_molecular) OVER (
		    PARTITION BY bulletin_date
			ORDER BY collected_date
			ROWS 6 PRECEDING
		) AS positive_molecular,
		sum(molecular) OVER (
		    PARTITION BY bulletin_date
			ORDER BY collected_date
			ROWS 6 PRECEDING
		) AS molecular
	FROM {{ ref('biostatistics_encounters_agg') }}
	INNER JOIN bulletins USING (bulletin_date)
)
SELECT
	bulletin_date AS "Datos",
	collected_date AS "Muestras",
	cases "Casos",
	antigens / 7.0 AS "Antígenos",
	100.0 * positive_antigens / antigens
		AS "Positividad antígenos",
	molecular / 7.0 AS "Moleculares",
	100.0 * positive_molecular / molecular
		AS "Positividad molecular",
	100.0 * (1.0 * positive_antigens / antigens)
		/ (1.0 * positive_molecular / molecular)
		AS "Razón de positividades"
FROM rates
ORDER BY collected_date DESC;