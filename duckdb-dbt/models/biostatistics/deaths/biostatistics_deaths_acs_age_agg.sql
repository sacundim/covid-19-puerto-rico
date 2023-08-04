--
-- Aggregated to the age ranges from the {{ ref('acs_2019_1y_age_ranges') }}
-- table, which aren't less detailed than Biostatistics deaths but (a) just
-- to be safe and (b) it brings in the ACS population estimates.
--
WITH grid AS (
    -- The underlying {{ ref('biostatistics_deaths_cube') }} table
    -- is sparse--for example, dates where no children died don't
    -- have rows in the table.  The charts where I use this data
    -- really want to have it as a dense grid where every
    -- combination of bulletin_date, death_date and age band is
    -- represented. So first we use sequence generation and CROSS
    -- JOINs to build a grid.
    WITH minmax AS (
        SELECT
            min(death_date) min_death_date,
            max(death_date) max_death_date,
            min(bulletin_date) AS min_bulletin_date,
            max(bulletin_date) AS max_bulletin_date
        FROM {{ ref('biostatistics_deaths_cube') }}
    )
    SELECT
        CAST(bulletin.bulletin_ts AS DATE) bulletin_date,
        CAST(death.death_ts AS DATE) death_date,
        age_gte AS acs_age_gte,
        age_lt AS acs_age_lt,
        population AS acs_population
    FROM minmax
    CROSS JOIN LATERAL (
        VALUES (generate_series(min_bulletin_date, max_bulletin_date, INTERVAL '1' DAY))
    ) AS bulletin_array (bulletin_ts_array)
    CROSS JOIN UNNEST(bulletin_ts_array) AS bulletin (bulletin_ts)
    CROSS JOIN LATERAL (
        VALUES (generate_series(min_death_date, max_death_date, INTERVAL '1' DAY))
    ) AS death_date_array (death_ts_array)
    CROSS JOIN UNNEST(death_ts_array) AS death (death_ts)
    CROSS JOIN {{ ref('acs_2019_1y_age_ranges') }}
    WHERE CAST(death.death_ts AS DATE) <= CAST(bulletin.bulletin_ts AS DATE)
)
SELECT
	grid.bulletin_date,
	grid.death_date,
	grid.acs_age_gte,
	grid.acs_age_lt,
	grid.acs_population,
   	coalesce(sum(deaths), 0) deaths,
   	sum(sum(deaths)) OVER (
   		PARTITION BY grid.bulletin_date, grid.acs_age_gte
   		ORDER BY grid.death_date
   	) AS cumulative_deaths
FROM {{ ref('biostatistics_deaths_cube') }} deaths
RIGHT OUTER JOIN grid
    ON grid.bulletin_date = deaths.bulletin_date
    AND grid.death_date = deaths.death_date
    AND grid.acs_age_gte <= deaths.age_gte
    AND deaths.age_gte < COALESCE(grid.acs_age_lt, 9999)
GROUP BY
	grid.bulletin_date,
	grid.death_date,
	grid.acs_age_gte,
	grid.acs_age_lt,
	grid.acs_population
ORDER BY
	grid.bulletin_date,
	grid.death_date,
    grid.acs_age_gte