WITH bulletins AS (
	SELECT max(bulletin_date) AS max_bulletin_date
	FROM {{ ref('bioportal_curve') }}
), rates AS (
	SELECT
		bulletin_date,
		bio.collected_date,
		sum(cases) OVER (
		    PARTITION BY bulletin_date
			ORDER BY bio.collected_date
			ROWS 6 PRECEDING
		) / 7.0 AS cases,
		100.0 * sum(bio.positive_antigens) OVER (
		    PARTITION BY bulletin_date
			ORDER BY bio.collected_date
			ROWS 6 PRECEDING
		) / lag(bio.antigens) OVER (
		    PARTITION BY bulletin_date
			ORDER BY bio.collected_date
			ROWS 6 PRECEDING
		) antigens,
		100.0 * sum(bio.positive_molecular) OVER (
		    PARTITION BY bulletin_date
			ORDER BY bio.collected_date
			ROWS 6 PRECEDING
		) / lag(bio.molecular) OVER (
		    PARTITION BY bulletin_date
			ORDER BY bio.collected_date
			ROWS 6 PRECEDING
		) molecular
	FROM {{ ref('bioportal_encounters_agg') }} bio
	INNER JOIN bulletins
		ON bulletins.max_bulletin_date = bio.bulletin_date
)
SELECT
	bulletin_date AS "Datos hasta",
	date_add('day', -6, collected_date) "Muestras desde",
	collected_date AS "Muestras hasta",
	cases "Casos",
	antigens "Positividad antígenos",
	molecular "Positividad molecular",
	100.0 * antigens / molecular "Razón"
FROM rates
ORDER BY collected_date DESC;
