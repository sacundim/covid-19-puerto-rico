WITH bulletins AS (
	SELECT max(bulletin_date) AS bulletin_date
	FROM {{ ref('bioportal_encounters_agg') }}
), rates AS (
	SELECT
		bulletin_date,
		bio.collected_date,
		bio.cumulative_molecular - lag(bio.cumulative_molecular) OVER (
			ORDER BY bio.collected_date
		) AS molecular,
		bio.cumulative_antigens - lag(bio.cumulative_antigens) OVER (
			ORDER BY bio.collected_date
		) AS antigens,
		bio.cumulative_positive_molecular - lag(bio.cumulative_positive_molecular) OVER (
			ORDER BY bio.collected_date
		) AS positive_molecular,
		bio.cumulative_positive_antigens - lag(bio.cumulative_positive_antigens) OVER (
			ORDER BY bio.collected_date
		) AS positive_antigens,
		(bio.cumulative_cases - lag(bio.cumulative_cases) OVER (
			ORDER BY bio.collected_date
		)) AS cases,
		bio.cumulative_molecular_cases - lag(bio.cumulative_molecular_cases) OVER (
			ORDER BY bio.collected_date
		) AS molecular_cases,
		bio.cumulative_antigens_cases - lag(bio.cumulative_antigens_cases) OVER (
			ORDER BY bio.collected_date
		) AS antigens_cases
	FROM {{ ref('bioportal_encounters_agg') }} bio
	INNER JOIN bulletins
		USING (bulletin_date)
	-- Athena/Presto modulo function is broken with negative numbers; mod(-1, 7) = -1.
	-- So we add seven, subtract, and then take mod 7.
	WHERE day_of_week(collected_date) % 7 = mod(day_of_week(bulletin_date) + 7 - 4, 7)
)
SELECT
	bulletin_date AS "Data up to",
	collected_date AS "Week ending",
	antigens AS "Ag tests",
	molecular AS "PCR tests",
	cases AS "Cases",
	100.0 * antigens_cases / cases AS "% detected w/Ag",
	100.0 * positive_antigens / antigens AS "Ag+ %",
	100.0 * positive_molecular / molecular AS "PCR+ %",
	100.0 * (1.0 * positive_antigens / antigens)
		/ (1.0 * positive_molecular / molecular) "Ag+ % / PCR+ %"
FROM rates
ORDER BY collected_date DESC;
