--
-- RecentAgeGroups chart
--

SELECT
	bulletin_date,
	collected_date,
	acs_age_gte AS youngest,
	encounters.acs_age_lt - 1 AS oldest,
	encounters.acs_population AS population,
	encounters,
	antigens,
	molecular,
	cases,
	antigens_cases,
	molecular_cases,
	deaths,
	positive_antigens,
	positive_molecular
FROM {{ ref('biostatistics_encounters_acs_age_agg') }} encounters
INNER JOIN {{ ref('biostatistics_deaths_acs_age_agg') }} deaths
    USING (bulletin_date, acs_age_gte)
WHERE collected_date = death_date
AND collected_date >= bulletin_date - INTERVAL 175 DAY
ORDER BY
	bulletin_date,
	collected_date,
	youngest