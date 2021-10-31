--
-- A case curve by age curve with Census ACS populations baked in
--

SELECT
	encounters.bulletin_date,
	collected_date,
	age_dim.age_gte,
	age_dim.age_lt,
	age_dim.population,
	sum(encounters) AS encounters,
	sum(molecular) AS molecular,
	sum(sum(molecular)) OVER (
		PARTITION BY encounters.bulletin_date, age_dim.age_gte
		ORDER BY collected_date
	) AS cumulative_molecular,
	sum(positive_molecular) AS positive_molecular,
	sum(sum(positive_molecular)) OVER (
		PARTITION BY encounters.bulletin_date, age_dim.age_gte
		ORDER BY collected_date
	) AS cumulative_positive_molecular,
	sum(antigens) AS antigens,
	sum(sum(antigens)) OVER (
		PARTITION BY encounters.bulletin_date, age_dim.age_gte
		ORDER BY collected_date
	) AS cumulative_antigens,
	sum(positive_antigens) AS positive_antigens,
	sum(sum(positive_antigens)) OVER (
		PARTITION BY encounters.bulletin_date, age_dim.age_gte
		ORDER BY collected_date
	) AS cumulative_positive_antigens,
	sum(cases) AS cases,
	sum(molecular_cases) AS molecular_cases,
	sum(antigens_cases) AS antigens_cases,
	COALESCE(sum(deaths), 0) AS deaths
FROM {{ ref('bioportal_encounters_cube') }} encounters
LEFT OUTER JOIN {{ ref('bioportal_deaths_age_agg') }} deaths
    ON deaths.bulletin_date = encounters.bulletin_date
    AND deaths.death_date = encounters.collected_date
    AND deaths.age_range = encounters.age_range
INNER JOIN {{ ref('bioportal_age_ranges') }} bio
	ON bio.age_range = encounters.age_range
INNER JOIN {{ ref('acs_2019_1y_age_ranges') }} age_dim
	ON age_dim.age_gte <= bio.age_gte
	AND bio.age_gte < COALESCE(age_dim.age_lt, 9999)
WHERE collected_date >= DATE '2020-03-13'
GROUP BY
	encounters.bulletin_date,
	collected_date,
	age_dim.age_gte,
	age_dim.age_lt,
	age_dim.population
ORDER BY
	bulletin_date,
	collected_date,
	age_dim.age_gte;
