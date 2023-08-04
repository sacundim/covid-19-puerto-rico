--
-- Aggregated to the age ranges from the {{ ref('acs_2019_1y_age_ranges') }}
-- table, which are less detailed than Biostatistics cases.
--
WITH grid AS (
    -- The underlying {{ ref('biostatistics_encounters_cube') }}
    -- table is sparse--for example, dates where no children died
    -- don't have rows in the table.  The charts where I use this
    -- data really want to have it as a dense grid where every
    -- combination of bulletin_date, collected_date and age band
    -- is represented. So first we use sequence generation and
    -- CROSS JOINs to build a grid.
    WITH minmax AS (
        SELECT
            min(collected_date) min_collected_date,
            max(collected_date) max_collected_date,
            min(bulletin_date) AS min_bulletin_date,
            max(bulletin_date) AS max_bulletin_date
        FROM {{ ref('biostatistics_encounters_cube') }}
    )
    SELECT
        CAST(bulletin.bulletin_ts AS DATE) bulletin_date,
        CAST(collected.collected_ts AS DATE) collected_date,
        age_gte AS acs_age_gte,
        age_lt AS acs_age_lt,
        population AS acs_population
    FROM minmax
    CROSS JOIN LATERAL (
        VALUES (generate_series(min_bulletin_date, max_bulletin_date, INTERVAL '1' DAY))
    ) AS bulletin_array (bulletin_ts_array)
    CROSS JOIN UNNEST(bulletin_ts_array) AS bulletin (bulletin_ts)
    CROSS JOIN LATERAL (
        VALUES (generate_series(min_collected_date, max_collected_date, INTERVAL '1' DAY))
    ) AS collected_date_array (collected_ts_array)
    CROSS JOIN UNNEST(collected_ts_array) AS collected (collected_ts)
    CROSS JOIN {{ ref('acs_2019_1y_age_ranges') }}
    WHERE CAST(collected.collected_ts AS DATE) <= CAST(bulletin.bulletin_ts AS DATE)
)
SELECT
    grid.bulletin_date,
	grid.collected_date,
	grid.acs_age_gte,
	grid.acs_age_lt,
	grid.acs_population,
	coalesce(sum(encounters), 0) encounters,
	coalesce(sum(cases), 0) cases,
	coalesce(sum(cases_strict), 0) cases_strict,
	coalesce(sum(first_infections), 0) first_infections,
	coalesce(sum(possible_reinfections), 0) possible_reinfections,
	coalesce(sum(rejections), 0) rejections,
	coalesce(sum(antigens), 0) antigens,
	coalesce(sum(molecular), 0) molecular,
	coalesce(sum(positive_antigens), 0) positive_antigens,
	coalesce(sum(positive_molecular), 0) positive_molecular,
	coalesce(sum(antigens_cases), 0) antigens_cases,
	coalesce(sum(molecular_cases), 0) molecular_cases,
	coalesce(sum(initial_molecular), 0) initial_molecular,
	coalesce(sum(initial_positive_molecular), 0) initial_positive_molecular
FROM {{ ref('biostatistics_encounters_cube') }} encounters
INNER JOIN {{ ref('acs_2019_1y_age_ranges') }} acs
    ON acs.age_gte <= encounters.age_gte
    AND encounters.age_gte < COALESCE(acs.age_lt, 9999)
RIGHT OUTER JOIN grid
    ON grid.bulletin_date = encounters.bulletin_date
    AND grid.collected_date = encounters.collected_date
    AND grid.acs_age_gte <= encounters.age_gte
    AND encounters.age_gte < COALESCE(grid.acs_age_lt, 9999)
GROUP BY
	grid.bulletin_date,
	grid.acs_age_gte,
	grid.acs_age_lt,
	grid.acs_population,
	grid.collected_date
WINDOW cumulative AS (
    PARTITION BY grid.bulletin_date, grid.acs_age_gte
    ORDER BY grid.collected_date
), delta AS (
    PARTITION BY grid.collected_date, grid.acs_age_gte
    ORDER BY grid.bulletin_date
)
ORDER BY
	grid.bulletin_date,
	grid.acs_age_gte,
	grid.collected_date
