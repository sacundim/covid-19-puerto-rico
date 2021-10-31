--
-- Cases by age group, both as raw numbers and by million population.
-- And when I say million population, I mean using Census Bureau
-- estimate of the population size for that age group.
--

SELECT
	bulletin_date,
	collected_date,
	population,
	age_gte AS youngest,
	age_lt - 1 AS oldest,
	encounters,
	1e6 * encounters / population
		AS encounters_1m,
	cases,
	1e6 * cases / population
		AS cases_1m,
	deaths,
	1e6 * deaths / population
		AS deaths_1m
FROM {{ ref('bioportal_acs_age_curve') }}
ORDER BY
	bulletin_date DESC,
	collected_date DESC,
	youngest;
