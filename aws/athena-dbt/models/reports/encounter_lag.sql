--
-- Encounters-based test and case lag.
--

SELECT
	bulletin_date,
	min(age) age_gte,
	max(age) + 1 age_lt,
	sum(delta_encounters) delta_encounters,
	sum(delta_cases) delta_cases,
	sum(delta_antigens) delta_antigens,
	sum(delta_antigens_cases) delta_antigens_cases,
	sum(delta_molecular) delta_molecular,
	sum(delta_molecular_cases) delta_molecular_cases
FROM {{ ref('biostatistics_encounters_agg') }}
WHERE age <= 20
AND bulletin_date >= DATE '2023-05-17'
GROUP BY
	bulletin_date,
	CASE
		WHEN 0 <= age AND age < 3 THEN age
		WHEN 3 <= age AND age < 7 THEN 3
		WHEN 7 <= age AND age < 14 THEN 14
		WHEN 14 <= age AND age < 21 THEN 21
	END
ORDER BY bulletin_date, age_lt;
