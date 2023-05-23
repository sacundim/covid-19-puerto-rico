--
-- Cases by age group, both as raw numbers and by million population.
-- And when I say million population, I mean using Census Bureau
-- estimate of the population size for that age group.
--

SELECT
	bulletin_date,
	collected_date,
	acs_age_gte AS youngest,
	encounters.acs_age_lt - 1 AS oldest,
	encounters.acs_population AS population,
	encounters,
	1e6 * encounters / encounters.acs_population
		AS encounters_1m,
	cases,
	1e6 * cases / encounters.acs_population
		AS cases_1m,
	deaths,
	1e6 * deaths / encounters.acs_population
		AS deaths_1m
FROM {{ ref('biostatistics_encounters_acs_age_agg') }} encounters
INNER JOIN {{ ref('biostatistics_deaths_acs_age_agg') }} deaths
    USING (downloaded_at, bulletin_date, acs_age_gte)
WHERE collected_date = death_date
ORDER BY
	bulletin_date,
	collected_date,
	acs_age_gte;
