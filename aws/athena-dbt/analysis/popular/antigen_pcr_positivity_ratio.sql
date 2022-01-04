WITH bulletins AS (
	SELECT max(bulletin_date) AS max_bulletin_date
	FROM {{ ref('bioportal_curve') }}
), rates AS (
	SELECT
		bulletin_date,
		bio.collected_date,
			(bio.cumulative_cases - lag(bio.cumulative_cases, 7) OVER (
		ORDER BY bio.collected_date
		)) cases,
		100.0 * (bio.cumulative_positive_antigens - lag(bio.cumulative_positive_antigens) OVER (
			ORDER BY bio.collected_date
		)) / (bio.cumulative_antigens - lag(bio.cumulative_antigens) OVER (
			ORDER BY bio.collected_date
		)) antigens,
		100.0 * (bio.cumulative_positive_molecular - lag(bio.cumulative_positive_molecular) OVER (
			ORDER BY bio.collected_date
		)) / (bio.cumulative_molecular - lag(bio.cumulative_molecular) OVER (
			ORDER BY bio.collected_date
		)) molecular
	FROM {{ ref('bioportal_encounters_agg') }} bio
	INNER JOIN bulletins
		ON bulletins.max_bulletin_date = bio.bulletin_date
	WHERE day_of_week(collected_date) = day_of_week(bulletin_date)
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
