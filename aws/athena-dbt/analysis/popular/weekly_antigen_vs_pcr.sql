WITH bulletins AS (
	SELECT max(bulletin_date) AS bulletin_date
	FROM {{ ref('bioportal_curve') }}
), rates AS (
	SELECT
		bulletin_date,
		bio.collected_date,
		(bio.cumulative_molecular - lag(bio.cumulative_molecular) OVER (
			ORDER BY bio.collected_date
		)) AS molecular,
		(bio.cumulative_antigens - lag(bio.cumulative_antigens) OVER (
			ORDER BY bio.collected_date
		)) AS antigens,
		(bio.cumulative_cases - lag(bio.cumulative_cases) OVER (
			ORDER BY bio.collected_date
		)) AS cases,
		100.0 * (bio.cumulative_antigens_cases - lag(bio.cumulative_antigens_cases) OVER (
			ORDER BY bio.collected_date
		)) / (bio.cumulative_cases - lag(bio.cumulative_cases) OVER (
			ORDER BY bio.collected_date
		)) pct_cases_antigens,
		100.0 * (bio.cumulative_positive_antigens - lag(bio.cumulative_positive_antigens) OVER (
			ORDER BY bio.collected_date
		)) / (bio.cumulative_antigens - lag(bio.cumulative_antigens) OVER (
			ORDER BY bio.collected_date
		)) antigen_positivity,
		100.0 * (bio.cumulative_positive_molecular - lag(bio.cumulative_positive_molecular) OVER (
			ORDER BY bio.collected_date
		)) / (bio.cumulative_molecular - lag(bio.cumulative_molecular) OVER (
			ORDER BY bio.collected_date
		)) molecular_positivity
	FROM {{ ref('bioportal_encounters_agg') }} bio
	INNER JOIN bulletins
		USING (bulletin_date)
	WHERE day_of_week(collected_date) % 7 = (day_of_week(bulletin_date) - 2) % 7
)
SELECT
	bulletin_date AS "Data up to",
	collected_date AS "Week ending",
	antigens "Ag tests",
	molecular "PCR tests",
	cases "Cases",
	pct_cases_antigens "% detected w/Ag",
	antigen_positivity "Ag positive %",
	molecular_positivity "PCR positive %",
	100.0 * antigen_positivity / molecular_positivity "Ratio"
FROM rates
ORDER BY collected_date DESC;
