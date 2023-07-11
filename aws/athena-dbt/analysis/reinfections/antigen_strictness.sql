--
-- Comparison between the case counts from the two followup test definitions.
-- The "strict" definition is what UKHSA adopted on Feb. 1, 2022, which counts
-- a positive test as a followup for the next 90 days after **any** positive
-- test, antigen or PCR.
--
-- One criticism of this is that this 90 day criterion is much too strict
-- for antigen tests, whose lower sentivity means they go negative soon after
-- an episode, and that thus the strict definition will miss possible reinfections
-- that we may reasonably spot through positive antigen tests.
--
WITH bulletins AS (
	SELECT max(bulletin_date) AS bulletin_date
	FROM {{ ref('biostatistics_encounters_agg') }}
)
SELECT
	bulletin_date "Datos",
	collected_date AS "Muestras",
	bio.cases "Casos",
	(bio.cumulative_cases - lag(bio.cumulative_cases, 7) OVER (
		ORDER BY bio.collected_date
	)) / 7.0 AS "Promedio",
	bio.cases_strict "Casos (estricto)",
	(bio.cumulative_cases_strict - lag(bio.cumulative_cases_strict, 7) OVER (
		ORDER BY bio.collected_date
	)) / 7.0 AS "Promedio",
	bio.cases - bio.cases_strict "Diferencia",
	((bio.cumulative_cases - bio.cumulative_cases_strict)
		- lag(bio.cumulative_cases - bio.cumulative_cases_strict, 7) OVER (
			ORDER BY bio.collected_date
		)) / 7.0 AS "Promedio"
FROM {{ ref('biostatistics_encounters_agg') }} bio
INNER JOIN bulletins
    USING (bulletin_date)
ORDER BY collected_date DESC;
