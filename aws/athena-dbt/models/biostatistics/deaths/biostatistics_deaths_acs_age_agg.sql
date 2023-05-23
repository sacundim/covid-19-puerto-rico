--
-- Aggregated to the age ranges from the {{ ref('acs_2019_1y_age_ranges') }}
-- table, which aren't less detailed than Biostatistics deaths but (a) just
-- to be safe and (b) it brings in the ACS population estimates.
--
SELECT
	downloaded_at,
	downloaded_date,
	bulletin_date,
	death_date,
	acs.age_gte AS acs_age_gte,
	acs.age_lt AS acs_age_lt,
	acs.population AS acs_population,
   	count(*) deaths,
   	sum(count(*)) OVER (
   		PARTITION BY downloaded_at, bulletin_date, acs.age_gte
   		ORDER BY death_date
   	) AS cumulative_deaths
FROM {{ ref('biostatistics_deaths_cube') }} deaths
INNER JOIN {{ ref('acs_2019_1y_age_ranges') }} acs
    ON acs.age_gte <= deaths.age_gte
    AND deaths.age_gte < COALESCE(acs.age_lt, 9999)
GROUP BY
	downloaded_at,
	downloaded_date,
	bulletin_date,
	death_date,
	acs.age_gte,
	acs.age_lt,
	acs.population
ORDER BY
	downloaded_at,
	downloaded_date,
	bulletin_date,
	death_date,
    acs.age_gte