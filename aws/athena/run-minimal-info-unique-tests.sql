--
-- The `minimal-info-unique-tests` row-per-test dataset.
-- We only run this occasionally for ad-hoc analyses.
--

MSCK REPAIR TABLE covid_pr_sources.minimal_info_unique_tests_parquet_v4;

DROP TABLE IF EXISTS covid_pr_etl.minimal_info_unique_tests;

CREATE TABLE covid_pr_etl.minimal_info_unique_tests WITH (
    format = 'PARQUET',
    bucketed_by = ARRAY['bulletin_date'],
    bucket_count = 4
) AS
WITH first_clean AS (
    SELECT
    	date(downloaded_date) AS downloaded_date,
        CAST(from_iso8601_timestamp(downloadedAt) AS TIMESTAMP)
            AS downloaded_at,
        CAST(from_iso8601_timestamp(downloadedAt) AS DATE) - INTERVAL '1' DAY
            AS bulletin_date,
        CAST(date_parse(nullif(collectedDate, ''), '%m/%d/%Y') AS DATE)
            AS raw_collected_date,
        CAST(date_parse(nullif(reportedDate, ''), '%m/%d/%Y') AS DATE)
            AS raw_reported_date,
        date_parse(createdAt, '%m/%d/%Y %H:%i') AS created_at,
        nullif(ageRange, '') AS age_range,
        CASE city
            WHEN '' THEN NULL
            WHEN 'Loiza' THEN 'Loíza'
            WHEN 'Rio Grande' THEN 'Río Grande'
            ELSE city
        END AS municipality,
	    testType AS raw_test_type,
        CASE
            WHEN testType IN (
                'Molecular', 'MOLECULAR'
            ) THEN 'Molecular'
            WHEN testType IN (
                'Antigens', 'ANTIGENO'
            ) THEN 'Antígeno'
            WHEN testType IN (
                'Serological', 'Serological IgG Only', 'Total Antibodies', 'SEROLOGICAL'
            ) THEN 'Serológica'
            ELSE testType
        END AS test_type,
        result,
        COALESCE(result, '') LIKE '%Positive%' AS positive
    FROM covid_pr_sources.minimal_info_unique_tests_parquet_v4
    -- IMPORTANT: This prunes partitions
    WHERE downloaded_date >= cast(date_add('day', -32, current_date) AS VARCHAR)
)
SELECT
    *,
    CASE
	    WHEN test_type IN ('Molecular')
	    THEN CASE
	        -- Null out nonsense collected dates. As of 2021-04-15,
	        -- out of over 1.9M PCR records there were only 267 with
	        -- `raw_collected_date` earlier than March 1 2020 and
	        -- 2,658 with nulls, so we don't really lose much.
	        WHEN raw_collected_date >= DATE '2020-03-01'
	        THEN raw_collected_date
	        -- This was the original method I used to clean up null
	        -- `collected_date` values, but now only for very early
	        -- dates.  Suggested originally by @rafalab; he uses two
	        -- days as the value and says that's the average, but my
	        -- spot check said 2.8 days so I use that.
	        WHEN DATE '2020-03-13' <= raw_reported_date
	                AND raw_reported_date <= DATE '2020-07-01'
	        THEN date_add('day', -3, raw_reported_date)
	    END
	    WHEN test_type IN ('Antígeno')
	    -- As of 2021-04-15, out of 652k antigen test records,
	    -- over 32k have `raw_collected_date` > `raw_reported_date`,
	    -- generally off by one day but some by a handful.  A lot of
	    -- the `raw_collected_date` look handwritten and approximate
	    -- like `2021-02-03 11:25:00` (five minute increments, no
	    -- seconds) while the `raw_reported_date` ones look computer
	    -- generated (second precision values).  I'm going to assume
	    -- that whichever of the two dates is earlier is likelier to
	    -- be right.
	    THEN least(coalesce(raw_collected_date, DATE '9999-12-31'),
	               coalesce(raw_reported_date, DATE '9999-12-31'))
	    ELSE coalesce(raw_collected_date, raw_reported_date, downloaded_date)
    END AS collected_date,
    CASE
	    WHEN test_type IN ('Molecular')
	    THEN CASE
	        WHEN raw_reported_date >= DATE '2020-03-13'
	        THEN raw_reported_date
	        ELSE downloaded_date
	    END
	    WHEN test_type IN ('Antígeno')
	    THEN greatest(coalesce(raw_collected_date, DATE '0001-01-01'),
	                  coalesce(raw_reported_date, DATE '0001-01-01'))
	    ELSE coalesce(raw_reported_date, raw_collected_date, downloaded_date)
    END AS reported_date
FROM first_clean;


DROP TABLE IF EXISTS covid_pr_etl.municipal_tests_cube;

CREATE TABLE covid_pr_etl.municipal_tests_cube WITH (
    format = 'PARQUET',
    bucketed_by = ARRAY['downloaded_at'],
    bucket_count = 1
) AS
SELECT
    downloaded_at,
    bulletin_date,
    collected_date,
    reported_date,
    municipality,
    test_type,
    count(*) AS specimens,
    count(*) FILTER (WHERE positive) AS positives
FROM covid_pr_etl.minimal_info_unique_tests
WHERE DATE '2020-03-01' <= collected_date
AND collected_date <= bulletin_date
AND DATE '2020-03-01' <= reported_date
AND reported_date <= bulletin_date
GROUP BY
    downloaded_at,
    bulletin_date,
    collected_date,
    reported_date,
    municipality,
    test_type;


DROP TABLE IF EXISTS covid_pr_etl.municipal_tests_collected_agg;

CREATE TABLE covid_pr_etl.municipal_tests_collected_agg WITH (
    format = 'PARQUET',
    bucketed_by = ARRAY['downloaded_at'],
    bucket_count = 1
) AS
SELECT
    downloaded_at,
    bulletin_date,
    collected_date,
    municipality,
    test_type,
    sum(specimens) AS specimens,
    sum(positives) AS positives,
    sum(sum(specimens)) OVER (
        PARTITION BY downloaded_at, bulletin_date, municipality, test_type
        ORDER BY collected_date
    ) AS cumulative_specimens,
    sum(sum(positives)) OVER (
        PARTITION BY downloaded_at, bulletin_date, municipality, test_type
        ORDER BY collected_date
    ) AS cumulative_positives,
    sum(specimens) - coalesce(lag(sum(specimens), 1, 0) OVER (
        PARTITION BY downloaded_at, bulletin_date, municipality, test_type
        ORDER BY collected_date
    ), 0) AS delta_specimens,
    sum(positives) - coalesce(lag(sum(positives), 1, 0) OVER (
            PARTITION BY downloaded_at, bulletin_date, municipality, test_type
            ORDER BY collected_date
        ), 0) AS delta_positives
FROM covid_pr_etl.municipal_tests_cube
GROUP BY
    downloaded_at,
    bulletin_date,
    collected_date,
    municipality,
    test_type;


-------------------------------------------------------------------------
-------------------------------------------------------------------------
--
-- Views
--

CREATE OR REPLACE VIEW covid_pr_etl.municipal_tests_per_capita AS
SELECT
    downloaded_at,
    bulletin_date,
    collected_date,
    municipality,
    population,
    sum(specimens) AS specimens,
    coalesce(sum(specimens) FILTER (
        WHERE test_type = 'Antígeno'
    ), 0) AS antigens,
    coalesce(sum(specimens) FILTER (
        WHERE test_type = 'Molecular'
    ), 0) AS molecular,
    sum(cumulative_specimens) AS cumulative_specimens,
    coalesce(sum(cumulative_specimens) FILTER (
        WHERE test_type = 'Antígeno'
    ), 0) AS cumulative_antigens,
    coalesce(sum(cumulative_specimens) FILTER (
        WHERE test_type = 'Molecular'
    ), 0) AS cumulative_molecular
FROM covid_pr_etl.municipal_tests_collected_agg tests
INNER JOIN covid_pr_sources.acs_2019_5y_municipal_race
    USING (municipality)
WHERE test_type IN ('Antígeno', 'Molecular')
GROUP BY
    downloaded_at,
    bulletin_date,
    municipality,
    collected_date,
    population
ORDER BY
    downloaded_at,
    bulletin_date,
    municipality,
    collected_date;

--
-- View for a map and/or scatterplot chart of antigen vs. molecular volume
--
CREATE OR REPLACE VIEW covid_pr_etl.municipal_testing_scatterplot AS
SELECT
	bulletin_date,
	municipality,
	abbreviation,
	population,
	sum(specimens) / 21.0 daily_specimens,
	1e3 * sum(specimens) / population / 21.0
		AS daily_specimens_1k,
	sum(antigens) / 21.0 daily_antigens,
	1e3 * sum(antigens) / population / 21.0
		AS daily_antigens_1k,
	sum(molecular) / 21.0 daily_molecular,
	1e3 * sum(molecular) / population / 21.0
		AS daily_molecular_1k
FROM covid_pr_etl.municipal_tests_per_capita
INNER JOIN covid_pr_sources.municipal_abbreviations
	USING (municipality)
WHERE date_add('day', -21, bulletin_date) < collected_date
AND collected_date <= bulletin_date
GROUP BY bulletin_date, municipality, abbreviation, population
ORDER BY bulletin_date DESC, municipality;


--
-- Big municipality data table for a SPLOM chart.  Depends on
-- municipal bulletin data and minimal-info-unique-tests.
--
DROP TABLE IF EXISTS covid_pr_etl.municipal_splom;
CREATE TABLE covid_pr_etl.municipal_splom WITH (
    format = 'PARQUET',
    bucketed_by = ARRAY['bulletin_date'],
    bucket_count = 1
) AS
WITH tests AS (
    SELECT
        bulletin_date,
        municipality,
        sum(specimens) cumulative_specimens,
        sum(specimens) FILTER (
            WHERE test_type = 'Antígeno'
        ) AS cumulative_antigens,
        sum(positives) FILTER (
            WHERE test_type = 'Antígeno'
        ) AS cumulative_positive_antigens,
        sum(specimens) FILTER (
            WHERE test_type = 'Molecular'
        ) AS cumulative_molecular,
        sum(positives) FILTER (
            WHERE test_type = 'Molecular'
        ) AS cumulative_positive_molecular
    FROM covid_pr_etl.municipal_tests_collected_agg
    INNER JOIN covid_pr_sources.municipal_abbreviations
        USING (municipality)
    WHERE test_type IN ('Antígeno', 'Molecular')
    GROUP BY bulletin_date, municipality
), cases AS (
	SELECT
        bulletin_date,
        municipality,
        sum(new_cases) AS cumulative_cases
	FROM covid19datos_v2_etl.cases_municipal_agg
	GROUP BY bulletin_date, municipality
)
SELECT
	local_date AS bulletin_date,
	municipio,
	race.fips,
	race.population,
	households_median,
	households_lt_10k_pct / 100.0
		AS households_lt_10k_pct,
    households_gte_200k_pct / 100.0
    	AS households_gte_200k_pct,
	white_alone,
	CAST(white_alone AS DOUBLE)
		/ race.population
		AS white_alone_pct,
	black_alone,
	CAST(black_alone AS DOUBLE)
		/ race.population
		AS black_alone_pct,
	cumulative_cases,
	1e3 * cumulative_cases / race.population
		AS cumulative_cases_1k,
	salud_total_dosis1 total_dosis1,
	CAST(salud_total_dosis1 AS DOUBLE)
		/ race.population
		AS total_dosis1_pct,
	salud_total_dosis2 total_dosis2,
	CAST(salud_total_dosis2 AS DOUBLE)
		/ race.population
		AS total_dosis2_pct,
	cumulative_specimens,
	1e3 * cumulative_specimens / race.population
		AS cumulative_specimens_1k,
	cumulative_antigens,
	1e3 * cumulative_antigens / race.population
		AS cumulative_antigens_1k,
	CAST(cumulative_positive_antigens AS DOUBLE)
		/ cumulative_antigens
		AS cumulative_antigen_positivity,
	cumulative_molecular,
	1e3 * cumulative_molecular / race.population
		AS cumulative_molecular_1k,
	CAST(cumulative_positive_molecular AS DOUBLE)
		/ cumulative_molecular
		AS cumulative_molecular_positivity
FROM tests
INNER JOIN covid19datos_v2_etl.municipal_vaccinations vax
	ON vax.local_date = tests.bulletin_date
	AND vax.municipio = tests.municipality
INNER JOIN cases
	USING (bulletin_date, municipality)
INNER JOIN covid_pr_sources.acs_2019_5y_municipal_race race
	ON vax.municipio = race.municipality
INNER JOIN covid_pr_sources.acs_2019_5y_municipal_household_income income
	ON vax.municipio = income.municipality
ORDER BY bulletin_date, municipio;