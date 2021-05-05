----------------------------------------------------------
----------------------------------------------------------
--
-- Rebuild the whole schema from scratch from the raw CSV tables.
--

DROP DATABASE IF EXISTS covid_pr_etl CASCADE;

CREATE DATABASE covid_pr_etl
LOCATION 's3://covid-19-puerto-rico-athena/';


----------------------------------------------------------
----------------------------------------------------------
--
-- The big core tables with disaggregated clean data.
--

--
-- The bitemporal daily bulletin cases table, with data
-- from the PRDoH daily PDF report
--
DROP TABLE IF EXISTS covid_pr_etl.bulletin_cases;
CREATE TABLE covid_pr_etl.bulletin_cases WITH (
    format = 'PARQUET',
    bucketed_by = ARRAY['bulletin_date'],
    bucket_count = 1
) AS
WITH cleaned AS (
    SELECT
        from_iso8601_date(bulletin_date) AS bulletin_date,
        from_iso8601_date(datum_date) AS datum_date,
        CAST(nullif(confirmed_cases, '') AS INTEGER) AS confirmed_cases,
        CAST(nullif(probable_cases, '') AS INTEGER) AS probable_cases,
        CAST(nullif(deaths, '') AS INTEGER) AS deaths
    FROM covid_pr_sources.bulletin_cases_csv
)
SELECT
    bulletin_date,
    datum_date,
	date_diff('day', datum_date, bulletin_date)
		AS age,
    confirmed_cases,
    sum(confirmed_cases) OVER (
        PARTITION BY bulletin_date
        ORDER BY datum_date
    ) AS cumulative_confirmed_cases,
    COALESCE(confirmed_cases, 0)
        - COALESCE(lag(confirmed_cases) OVER (
            PARTITION BY datum_date
            ORDER BY bulletin_date
        ), 0) AS delta_confirmed_cases,
    probable_cases,
    sum(probable_cases) OVER (
        PARTITION BY bulletin_date
        ORDER BY datum_date
    ) AS cumulative_probable_cases,
    COALESCE(probable_cases, 0)
        - COALESCE(lag(probable_cases) OVER (
            PARTITION BY datum_date
            ORDER BY bulletin_date
        ), 0) AS delta_probable_cases,
    deaths,
    sum(deaths) OVER (
        PARTITION BY bulletin_date
        ORDER BY datum_date
    ) AS cumulative_deaths,
    COALESCE(deaths, 0)
        - COALESCE(lag(deaths) OVER (
            PARTITION BY datum_date
            ORDER BY bulletin_date
        ), 0) AS delta_deaths
FROM cleaned;


--
-- HHS hospitals data set
--
DROP TABLE IF EXISTS covid_pr_etl.hhs_hospitals;
CREATE TABLE covid_pr_etl.hhs_hospitals WITH (
    format = 'PARQUET',
    bucketed_by = ARRAY['date'],
    bucket_count = 1
) AS
WITH max_timeseries_date AS (
	SELECT
		max(file_timestamp) AS max_file_timestamp,
		max(date) AS max_date
	FROM covid_hhs_sources.reported_hospital_utilization_timeseries_PR
)
SELECT hist.*
FROM covid_hhs_sources.reported_hospital_utilization_timeseries_PR hist
INNER JOIN max_timeseries_date
	ON file_timestamp = max_file_timestamp
UNION ALL
SELECT daily.*
FROM covid_hhs_sources.reported_hospital_utilization_PR daily
INNER JOIN max_timeseries_date
	ON date > max_date
ORDER BY date DESC;


--
-- Municipal vaccinations according to PRDoH
--
MSCK REPAIR TABLE covid_hhs_sources.vaccination_county_condensed_data_json;

DROP TABLE IF EXISTS covid_pr_etl.municipal_vaccinations;
CREATE TABLE covid_pr_etl.municipal_vaccinations WITH (
    format = 'PARQUET',
    bucketed_by = ARRAY['bulletin_date'],
    bucket_count = 1
) AS
SELECT
	local_date AS bulletin_date,
	municipio,
	population,
	total_dosis1,
	CAST(total_dosis1 AS DOUBLE)
		/ population
		AS total_dosis1_pct,
	total_dosis2,
	CAST(total_dosis2 AS DOUBLE)
		/ population
		AS total_dosis2_pct
FROM covid19datos_sources.vacunaciones_municipios_totales_daily vax
INNER JOIN covid_pr_sources.acs_2019_5y_municipal_race race
	ON vax.municipio = race.municipality
ORDER BY bulletin_date;


----------------------------------------------------------------------------------

--
-- Deaths according to Bioportal, which I don't think is as reliable
-- as the daily report.
--
MSCK REPAIR TABLE covid_pr_sources.deaths_parquet_v1;

CREATE TABLE covid_pr_etl.bioportal_deaths WITH (
    format = 'PARQUET',
    bucketed_by = ARRAY['downloaded_date'],
    bucket_count = 1
) AS
WITH first_clean AS (
	SELECT
		date(downloaded_date) AS downloaded_date,
	    CAST(from_iso8601_timestamp(downloadedAt) AS TIMESTAMP)
	        AS downloaded_at,
	    CAST(from_iso8601_timestamp(downloadedAt) AS DATE) - INTERVAL '1' DAY
	        AS bulletin_date,
	    date(from_iso8601_timestamp(nullif(deathDate, '')) AT TIME ZONE 'America/Puerto_Rico')
	        AS raw_death_date,
	    date(from_iso8601_timestamp(nullif(reportDate, '')) AT TIME ZONE 'America/Puerto_Rico')
	        AS report_date,
	    region,
	    sex,
	   	ageRange AS age_range
	FROM covid_pr_sources.deaths_parquet_v1
)
SELECT
	*,
	CASE
		WHEN raw_death_date < DATE '2020-01-01' AND month(raw_death_date) >= 3
		THEN from_iso8601_date(date_format(raw_death_date, '2020-%m-%d'))
		WHEN raw_death_date BETWEEN DATE '2020-01-01' AND DATE '2020-03-01'
		THEN date_add('year', 1, raw_death_date)
		ELSE raw_death_date
	END AS death_date
FROM first_clean
ORDER BY bulletin_date, death_date, report_date;

CREATE TABLE covid_pr_etl.bioportal_deaths_age_agg WITH (
    format = 'PARQUET',
    bucketed_by = ARRAY['downloaded_at'],
    bucket_count = 1
) AS
SELECT
	downloaded_at,
	bulletin_date,
	death_date,
   	age_range,
   	count(*) deaths,
   	sum(count(*)) OVER (
   		PARTITION BY downloaded_at, bulletin_date, age_range
   		ORDER BY death_date
   	) AS cumulative_deaths
FROM covid_pr_etl.bioportal_deaths
GROUP BY downloaded_at, bulletin_date, death_date, age_range
ORDER BY downloaded_at, bulletin_date, death_date, age_range;

CREATE TABLE covid_pr_etl.bioportal_deaths_agg WITH (
    format = 'PARQUET',
    bucketed_by = ARRAY['downloaded_at'],
    bucket_count = 1
) AS
SELECT
	downloaded_at,
	bulletin_date,
	death_date,
   	count(*) deaths,
   	sum(count(*)) OVER (
   		PARTITION BY downloaded_at, bulletin_date
   		ORDER BY death_date
   	) AS cumulative_deaths
FROM covid_pr_etl.bioportal_deaths
GROUP BY downloaded_at, bulletin_date, death_date
ORDER BY downloaded_at, bulletin_date, death_date;


--
-- The `orders/basic` row-per-test dataset from Bioportal.
--
MSCK REPAIR TABLE covid_pr_sources.orders_basic_parquet_v2;

CREATE TABLE covid_pr_etl.bioportal_orders_basic WITH (
    format = 'PARQUET',
    bucketed_by = ARRAY['downloaded_date'],
    bucket_count = 1
) AS
WITH downloads AS (
	SELECT
		max(downloadedAt) max_downloaded_at
	FROM covid_pr_sources.orders_basic_parquet_v2
), first_clean AS (
	SELECT
	    CAST(from_iso8601_timestamp(downloadedAt) AS TIMESTAMP)
	        AS downloaded_at,
	    CAST(from_iso8601_timestamp(nullif(collectedDate, '')) AS TIMESTAMP)
	    	AS raw_collected_at,
	    CAST(from_iso8601_timestamp(nullif(reportedDate, '')) AS TIMESTAMP)
	    	AS raw_reported_at,
	    CAST(from_iso8601_timestamp(resultCreatedAt) AS TIMESTAMP)
	    	AS result_created_at,
	    CAST(from_iso8601_timestamp(orderCreatedAt) AS TIMESTAMP)
	    	AS order_created_at,
	    date(from_iso8601_timestamp(downloadedAt) AT TIME ZONE 'America/Puerto_Rico')
	        AS downloaded_date,
	    date(from_iso8601_timestamp(resultCreatedAt) AT TIME ZONE 'America/Puerto_Rico')
	        AS received_date,
	    date(from_iso8601_timestamp(nullif(collectedDate, '')) AT TIME ZONE 'America/Puerto_Rico')
	        AS raw_collected_date,
	    date(from_iso8601_timestamp(nullif(reportedDate, '')) AT TIME ZONE 'America/Puerto_Rico')
	        AS raw_reported_date,
	    from_hex(replace(nullif(patientId, ''), '-')) AS patient_id,
	    nullif(ageRange, '') AS age_range,
	    nullif(region, '') AS region,
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
	FROM covid_pr_sources.orders_basic_parquet_v2 tests
	INNER JOIN downloads
	    ON downloads.max_downloaded_at = tests.downloadedAt
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
	    ELSE coalesce(raw_collected_date, raw_reported_date, received_date)
    END AS collected_date,
    CASE
	    WHEN test_type IN ('Molecular')
	    THEN CASE
	        WHEN raw_reported_date >= DATE '2020-03-13'
	        THEN raw_reported_date
	        ELSE received_date
	    END
	    WHEN test_type IN ('Antígeno')
	    THEN greatest(coalesce(raw_collected_date, DATE '0001-01-01'),
	                  coalesce(raw_reported_date, DATE '0001-01-01'))
	    ELSE coalesce(raw_reported_date, raw_collected_date, received_date)
    END AS reported_date
FROM first_clean;


----------------------------------------------------------
----------------------------------------------------------
--
-- # Encounters and followup analysis
--
-- This table takes all the antigen and PCR tests (no serology)
-- and does the following cleanup and enrichment:
--
-- 1. Eliminates duplicate tests for the same patient on the
--    same date. If any of the tests on one day is positive,
--    we classify the patient as a positive on that day.  We
--    call these "test encounters," a term used by the COVID
--    Tracking Project.
--
-- See: https://covidtracking.com/analysis-updates/test-positivity-in-the-us-is-a-mess
--
-- 2. Flags "followup" tests—tests such that the same patient
--    had a positive test no more than 90 days earlier. We use
--    a three month cutoff following the Council of State and
--    Territorial Epidemiologists (CSTE)'s 2020 Interim Case
--    Definition (Interim-20-ID-02, approved August 5, 2020),
--    which recommends this criterion for distinguishing new
--    cases from previous ones for the same patient.
--
-- See: https://wwwn.cdc.gov/nndss/conditions/coronavirus-disease-2019-covid-19/case-definition/2020/08/05/
--
CREATE TABLE covid_pr_etl.bioportal_encounters WITH (
    format = 'PARQUET',
    bucketed_by = ARRAY['downloaded_date'],
    bucket_count = 1
) AS
WITH downloads AS (
	SELECT
		max(downloaded_at) max_downloaded_at,
		max(downloaded_date) max_downloaded_date
	FROM covid_pr_etl.bioportal_orders_basic
)
SELECT
	cur.downloaded_at,
	cur.downloaded_date,
	cur.collected_date,
	cur.patient_id,
	max_by(cur.age_range, cur.received_date) AS age_range,
	max_by(cur.region, cur.received_date) AS region,
	min(cur.received_date) AS min_received_date,
	max(cur.received_date) AS max_received_date,
	-- True if and only if at least one specimen for that
	-- patient on that date came back positive, irrespective
	-- of the type of test.
	bool_or(cur.positive) positive,
	-- These two are true if and only if there is at least one
	-- specimen for that patient on that date was of the respective
	-- type, irrespective of positive and negative.
	bool_or(cur.test_type = 'Molecular') has_molecular,
	bool_or(cur.test_type = 'Antígeno') has_antigens,
	-- These two true are if and only if there is at least one
	-- specimen that is both of the respective type and came
	-- back positive.  Note that for example this means that
	-- `has_positive_antigens` is't synonymous with
	-- `positive AND has_antigens`, because that could be true 
	-- because the patient has a negative antigen and a positive
	-- molecular test on that date.
	bool_or(cur.test_type = 'Molecular' AND cur.positive)
	    AS has_positive_molecular,
	bool_or(cur.test_type = 'Antígeno' AND cur.positive)
	    AS has_positive_antigens,
    -- A followup test is any test—positive or negative—such
    -- that the same patient had a positive test in the 90
    -- days before. 
	COALESCE(bool_or(prev.collected_date >= date_add('day', -90, cur.collected_date)
			            AND prev.positive),
             FALSE)
		AS followup
FROM covid_pr_etl.bioportal_orders_basic cur
INNER JOIN downloads
    ON cur.downloaded_at = downloads.max_downloaded_at
    AND cur.downloaded_date = downloads.max_downloaded_date
LEFT OUTER JOIN covid_pr_etl.bioportal_orders_basic prev
	ON prev.test_type IN ('Molecular', 'Antígeno')
	AND prev.downloaded_at = cur.downloaded_at
	AND prev.downloaded_date = cur.downloaded_date
	AND prev.patient_id = cur.patient_id
	AND prev.collected_date < cur.collected_date
	AND DATE '2020-03-01' <= prev.collected_date
	AND prev.collected_date <= prev.received_date
	AND DATE '2020-03-01' <= prev.reported_date
	AND prev.reported_date <= prev.received_date
WHERE cur.test_type IN ('Molecular', 'Antígeno')
AND DATE '2020-03-01' <= cur.collected_date
AND cur.collected_date <= cur.received_date
AND DATE '2020-03-01' <= cur.reported_date
AND cur.reported_date <= cur.received_date
GROUP BY
	cur.downloaded_at,
	cur.downloaded_date,
	cur.collected_date,
	cur.patient_id;


--
-- Aggregates tables built off `bioportal_encounters`
--

CREATE TABLE covid_pr_etl.bioportal_encounters_cube WITH (
    format = 'PARQUET',
    bucketed_by = ARRAY['bulletin_date'],
    bucket_count = 1
) AS
WITH downloads AS (
	SELECT
		max(downloaded_at) max_downloaded_at,
		max(downloaded_date) max_downloaded_date
	FROM covid_pr_etl.bioportal_orders_basic
), bulletins AS (
	SELECT CAST(date_column AS DATE) AS bulletin_date
	FROM (
		VALUES (SEQUENCE(DATE '2020-04-24', DATE '2021-12-31', INTERVAL '1' DAY))
	) AS date_array (date_array)
	CROSS JOIN UNNEST(date_array) AS t2(date_column)
	INNER JOIN downloads
	    ON CAST(date_column AS DATE) < downloads.max_downloaded_date
), grouped AS (
    SELECT
        bulletins.bulletin_date,
        tests.collected_date,
        tests.age_range,
        count(*) AS encounters,
        -- A case is a test encounter that had a positive test and
        -- is not a followup to an earlier positive encounter.
        count(*) FILTER (
            WHERE tests.positive
            AND NOT tests.followup
        ) cases,
        -- An antigens case is a test encounter that had a positive antigens
        -- test and is not a followup to an earlier positive encounter.
        count(*) FILTER (
            WHERE tests.has_positive_antigens
            AND NOT tests.followup
        ) antigens_cases,
        -- A molecular case is a test encounter that had a positive PCR
        -- test, no positive antigen test, and is not a followup to an
        -- earlier positive encounter.
        count(*) FILTER (
            WHERE tests.has_positive_molecular
            AND NOT tests.has_positive_antigens
            AND NOT tests.followup
        ) molecular_cases,
        -- A rejected case is a non-followup encounter that had no
        -- positive tests and at least one of the tests is PCR.
        count(*) FILTER (
            WHERE tests.has_molecular
            AND NOT tests.followup
        ) rejections,
        -- Note that `has_antigens` and `has_molecular` don't
        -- have to add up to `encounters` because a person may
        -- get both test types the same day. Similar remarks
        -- apply to many of the sums below.
        count(*) FILTER (
            WHERE tests.has_antigens
        ) antigens,
        count(*) FILTER (
            WHERE tests.has_molecular
        ) molecular,
        -- These two are encounters where there was at least one 
        -- positive test of the respective type.  Note that for
        -- example `has_positive_antigens` isn't synonymos with
        -- `positive AND has_antigens`, because a patient could
        -- have a negative antigen and a positive PCR the same day.
        count(*) FILTER (
            WHERE tests.has_positive_antigens
        ) positive_antigens,
        count(*) FILTER (
            WHERE tests.has_positive_molecular
        ) positive_molecular,
        -- Non-followup test encounters where there was at least
        -- one molecular test.  These are, I claim, the most 
        -- appropriate for a positive rate calculation.
        count(*) FILTER (
            WHERE NOT tests.followup
            AND tests.has_molecular
        ) AS initial_molecular,
        -- Non-followup test encounters where there was at least
        -- one molecular test that came back positive.  These are,
        -- I claim, the most appropriate for a positive rate calculation.
        count(*) FILTER (
            WHERE NOT tests.followup
            AND tests.has_positive_molecular
        ) AS initial_positive_molecular
    FROM covid_pr_etl.bioportal_encounters tests
    INNER JOIN downloads
        ON tests.downloaded_at = downloads.max_downloaded_at
    INNER JOIN bulletins
        ON bulletins.bulletin_date < downloads.max_downloaded_date
        AND tests.min_received_date <= bulletins.bulletin_date
    GROUP BY
        bulletins.bulletin_date,
        tests.collected_date,
        tests.age_range
)
SELECT
    *,
    sum(encounters) OVER (
        PARTITION BY bulletin_date, age_range
        ORDER BY collected_date
    ) AS cumulative_encounters,
    sum(cases) OVER (
        PARTITION BY bulletin_date, age_range
        ORDER BY collected_date
    ) AS cumulative_cases,
    sum(rejections) OVER (
        PARTITION BY bulletin_date, age_range
        ORDER BY collected_date
    ) AS cumulative_rejections,
    sum(antigens) OVER (
        PARTITION BY bulletin_date, age_range
        ORDER BY collected_date
    ) AS cumulative_antigens,
    sum(molecular) OVER (
        PARTITION BY bulletin_date, age_range
        ORDER BY collected_date
    ) AS cumulative_molecular,
    sum(positive_antigens) OVER (
        PARTITION BY bulletin_date, age_range
        ORDER BY collected_date
    ) AS cumulative_positive_antigens,
    sum(positive_molecular) OVER (
        PARTITION BY bulletin_date, age_range
        ORDER BY collected_date
    ) AS cumulative_positive_molecular,
    sum(initial_molecular) OVER (
        PARTITION BY bulletin_date, age_range
        ORDER BY collected_date
    ) AS cumulative_initial_molecular,
    sum(antigens_cases) OVER (
        PARTITION BY bulletin_date, age_range
        ORDER BY collected_date
    ) AS cumulative_antigens_cases,
    sum(molecular_cases) OVER (
        PARTITION BY bulletin_date, age_range
        ORDER BY collected_date
    ) AS cumulative_molecular_cases,
    sum(initial_positive_molecular) OVER (
        PARTITION BY bulletin_date, age_range
        ORDER BY collected_date
    ) AS cumulative_initial_positive_molecular
FROM grouped
ORDER BY
	bulletin_date,
	collected_date,
	age_range;


CREATE TABLE covid_pr_etl.bioportal_encounters_agg WITH (
    format = 'PARQUET',
    bucketed_by = ARRAY['bulletin_date'],
    bucket_count = 1
) AS
SELECT
    bulletin_date,
	collected_date,
	date_diff('day', collected_date, bulletin_date) age,
	sum(encounters) encounters,
	sum(cases) cases,
	sum(rejections) rejections,
	sum(antigens) antigens,
	sum(molecular) molecular,
	sum(positive_antigens) positive_antigens,
	sum(positive_molecular) positive_molecular,
	sum(antigens_cases) antigens_cases,
	sum(molecular_cases) molecular_cases,
	sum(initial_molecular) initial_molecular,
	sum(initial_positive_molecular) initial_positive_molecular,
	sum(sum(encounters)) OVER (
	    PARTITION BY bulletin_date
	    ORDER BY collected_date
	) AS cumulative_encounters,
	sum(sum(cases)) OVER (
	    PARTITION BY bulletin_date
	    ORDER BY collected_date
	) AS cumulative_cases,
	sum(sum(rejections)) OVER (
	    PARTITION BY bulletin_date
	    ORDER BY collected_date
	) AS cumulative_rejections,
	sum(sum(antigens)) OVER (
	    PARTITION BY bulletin_date
	    ORDER BY collected_date
	) AS cumulative_antigens,
	sum(sum(molecular)) OVER (
	    PARTITION BY bulletin_date
	    ORDER BY collected_date
	) AS cumulative_molecular,
	sum(sum(positive_antigens)) OVER (
	    PARTITION BY bulletin_date
	    ORDER BY collected_date
	) AS cumulative_positive_antigens,
	sum(sum(positive_molecular)) OVER (
	    PARTITION BY bulletin_date
	    ORDER BY collected_date
	) AS cumulative_positive_molecular,
	sum(sum(antigens_cases)) OVER (
	    PARTITION BY bulletin_date
	    ORDER BY collected_date
	) AS cumulative_antigens_cases,
	sum(sum(molecular_cases)) OVER (
	    PARTITION BY bulletin_date
	    ORDER BY collected_date
	) AS cumulative_molecular_cases,
	sum(sum(initial_molecular)) OVER (
	    PARTITION BY bulletin_date
	    ORDER BY collected_date
	) AS cumulative_initial_molecular,
	sum(sum(initial_positive_molecular)) OVER (
	    PARTITION BY bulletin_date
	    ORDER BY collected_date
	) AS cumulative_initial_positive_molecular,
	sum(encounters) - lag(sum(encounters), 1, 0) OVER (
	    PARTITION BY collected_date
	    ORDER BY bulletin_date
	) AS delta_encounters,
	sum(cases) - lag(sum(cases), 1, 0) OVER (
	    PARTITION BY collected_date
	    ORDER BY bulletin_date
	) AS delta_cases,
	sum(rejections) - lag(sum(rejections), 1, 0) OVER (
	    PARTITION BY collected_date
	    ORDER BY bulletin_date
	) AS delta_rejections,
	sum(antigens) - lag(sum(antigens), 1, 0) OVER (
	    PARTITION BY collected_date
	    ORDER BY bulletin_date
	) AS delta_antigens,
	sum(molecular) - lag(sum(molecular), 1, 0) OVER (
	    PARTITION BY collected_date
	    ORDER BY bulletin_date
	) AS delta_molecular,
	sum(positive_antigens) - lag(sum(positive_antigens), 1, 0) OVER (
	    PARTITION BY collected_date
	    ORDER BY bulletin_date
	) AS delta_positive_antigens,
	sum(positive_molecular) - lag(sum(positive_molecular), 1, 0) OVER (
	    PARTITION BY collected_date
	    ORDER BY bulletin_date
	) AS delta_positive_molecular,
	sum(antigens_cases) - lag(sum(antigens_cases), 1, 0) OVER (
	    PARTITION BY collected_date
	    ORDER BY bulletin_date
	) AS delta_antigens_cases,
	sum(molecular_cases) - lag(sum(molecular_cases), 1, 0) OVER (
	    PARTITION BY collected_date
	    ORDER BY bulletin_date
	) AS delta_molecular_cases,
	sum(initial_molecular) - lag(sum(initial_molecular), 1, 0) OVER (
	    PARTITION BY collected_date
	    ORDER BY bulletin_date
	) AS delta_initial_molecular,
	sum(initial_positive_molecular) - lag(sum(initial_positive_molecular), 1, 0) OVER (
	    PARTITION BY collected_date
	    ORDER BY bulletin_date
	) AS delta_initial_positive_molecular
FROM covid_pr_etl.bioportal_encounters_cube
GROUP BY
	bulletin_date,
	collected_date;


--
-- A case curve from Bioportal data. This doesn't agree with the
-- official reports' cases curve for a few reasons:
--
-- 1. The deduplication in Bioportal's `patientId` field doesn't
--    work the same as the official bulletin, and in fact gives
--    very different results;
-- 2. Bioportal has fresher data than the official bulletin,
--    routinely by 2-3 days;
-- 3. This curve strives to use all data that Bioportal provides,
--    not just molecular test results; we will definitely count
--    antigen positives toward cases, and [TODO] count serological
--    tests toward adjudicating the earliest collected_date for
--    a case if there is molecular confirmation soon thereafter.
--
CREATE OR REPLACE VIEW covid_pr_etl.bioportal_curve AS
SELECT
	bulletin_date,
	collected_date,
	cases,
    sum(cases) OVER (
		PARTITION BY bulletin_date
		ORDER BY collected_date
	) AS cumulative_cases,
	cases - coalesce(lag(cases) OVER (
		PARTITION BY collected_date
		ORDER BY bulletin_date
	), 0) AS delta_cases
FROM covid_pr_etl.bioportal_encounters_agg
ORDER BY bulletin_date DESC, collected_date DESC;

--
-- A case curve by age curve with Census ACS populations baked in
--
CREATE TABLE covid_pr_etl.bioportal_acs_age_curve WITH (
    format = 'PARQUET',
    bucketed_by = ARRAY['bulletin_date'],
    bucket_count = 1
) AS
SELECT
	encounters.bulletin_date,
	collected_date,
	age_dim.age_gte,
	age_dim.age_lt,
	age_dim.population,
	sum(cases) AS cases,
	COALESCE(sum(deaths), 0) AS deaths
FROM covid_pr_etl.bioportal_encounters_cube encounters
LEFT OUTER JOIN covid_pr_etl.bioportal_deaths_age_agg deaths
    ON deaths.bulletin_date = encounters.bulletin_date
    AND deaths.death_date = encounters.collected_date
    AND deaths.age_range = encounters.age_range
INNER JOIN covid_pr_sources.bioportal_age_ranges bio
	ON bio.age_range = encounters.age_range
INNER JOIN covid_pr_sources.acs_2019_1y_age_ranges age_dim
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


----------------------------------------------------------
----------------------------------------------------------
--
-- # Specimens analysis
--
-- These perform fairly straightforward aggregation of
-- Bioportal data, without deduplicating test specimens
-- taken during the same test encounter.  For a definition
-- of "specimen" vs. "encounter" see:
--
-- * https://covidtracking.com/analysis-updates/test-positivity-in-the-us-is-a-mess
--

--
-- Counts of tests from Bioportal, classified along four
-- time axes:
--
-- * `bulletin_date`, which is the data as-of date (that
--   allows us to "rewind" data to earlier state);
--
-- * `collected_date`, which is when test samples were taken
--
-- * `reported_date`, which is when the laboratory knew the
--   test result (but generally earlier than it communicated
--   it to PRDoH).
--
-- * `received_date`, which is when Bioportal says they received
--   the actual test result.
--
-- Yes, I know the name says "tritemporal" and now it's four
-- time axes.  Not gonna rename it right now.
--
CREATE TABLE covid_pr_etl.bioportal_tritemporal_counts WITH (
    format = 'PARQUET',
    bucketed_by = ARRAY['bulletin_date'],
    bucket_count = 1
) AS
WITH downloads AS (
	SELECT
		max(downloaded_at) max_downloaded_at,
		max(downloaded_date) max_downloaded_date
	FROM covid_pr_etl.bioportal_orders_basic
), bulletins AS (
	SELECT CAST(date_column AS DATE) AS bulletin_date
	FROM (
		VALUES (SEQUENCE(DATE '2020-04-24', DATE '2021-12-31', INTERVAL '1' DAY))
	) AS date_array (date_array)
	CROSS JOIN UNNEST(date_array) AS t2(date_column)
	INNER JOIN downloads
	    ON CAST(date_column AS DATE) < downloads.max_downloaded_date
)
SELECT
	test_type,
	bulletins.bulletin_date,
	reported_date,
	collected_date,
	received_date,
	count(*) tests,
	count(*) FILTER (WHERE positive)
		AS positive_tests
FROM covid_pr_etl.bioportal_orders_basic tests
INNER JOIN downloads
	ON tests.downloaded_at = downloads.max_downloaded_at
INNER JOIN bulletins
	ON bulletins.bulletin_date < downloads.max_downloaded_date
	AND tests.received_date <= bulletins.bulletin_date
AND DATE '2020-03-01' <= collected_date
AND collected_date <= received_date
AND DATE '2020-03-01' <= reported_date
AND reported_date <= received_date
GROUP BY test_type, bulletins.bulletin_date, collected_date, reported_date, received_date;


--
-- Same data as `bioportal_tritemporal_counts`, but enriched with
-- daily data changes (how many tests were reported from one
-- `bulletin_date` to the next).
--
CREATE TABLE covid_pr_etl.bioportal_tritemporal_deltas WITH (
    format = 'PARQUET',
    bucketed_by = ARRAY['bulletin_date'],
    bucket_count = 1
) AS
SELECT
	test_type,
	bulletin_date,
	received_date,
	reported_date,
	collected_date,
	tests,
	tests - COALESCE(lag(tests) OVER (
        PARTITION BY test_type, collected_date, reported_date, received_date
	    ORDER BY bulletin_date
    ), 0) AS delta_tests,
	positive_tests,
	positive_tests - COALESCE(lag(positive_tests) OVER (
        PARTITION BY test_type, collected_date, reported_date, received_date
	    ORDER BY bulletin_date
    ), 0) AS delta_positive_tests
FROM covid_pr_etl.bioportal_tritemporal_counts
WHERE collected_date <= bulletin_date
AND reported_date <= bulletin_date;


--
-- Same data as `bioportal_tritemporal_deltas`, but aggregated
-- to `collected_date` (i.e., removes `reported_date` and
-- `received_date`).
--
CREATE TABLE covid_pr_etl.bioportal_collected_agg WITH (
    format = 'PARQUET',
    bucketed_by = ARRAY['bulletin_date'],
    bucket_count = 1
) AS
SELECT
	test_type,
	bulletin_date,
	collected_date,
	date_diff('day', collected_date, bulletin_date)
		AS collected_age,
	sum(tests) AS tests,
	sum(sum(tests)) OVER (
        PARTITION BY test_type, bulletin_date
        ORDER BY collected_date
    ) AS cumulative_tests,
	sum(delta_tests) AS delta_tests,
	sum(positive_tests) AS positive_tests,
	sum(sum(positive_tests)) OVER (
        PARTITION BY test_type, bulletin_date
        ORDER BY collected_date
    ) AS cumulative_positives,
	sum(delta_positive_tests) AS delta_positive_tests
FROM covid_pr_etl.bioportal_tritemporal_deltas
GROUP BY test_type, bulletin_date, collected_date;


--
-- Same data as `bioportal_tritemporal_deltas`, but aggregated
-- to `reported_date`.
--
CREATE TABLE covid_pr_etl.bioportal_reported_agg WITH (
    format = 'PARQUET',
    bucketed_by = ARRAY['bulletin_date'],
    bucket_count = 1
) AS
SELECT
	test_type,
	bulletin_date,
	reported_date,
	date_diff('day', reported_date, bulletin_date)
		AS reported_age,
	sum(tests) AS tests,
	sum(sum(tests)) OVER (
        PARTITION BY test_type, bulletin_date
        ORDER BY reported_date
    ) AS cumulative_tests,
	sum(delta_tests) AS delta_tests,
	sum(positive_tests) AS positive_tests,
	sum(sum(positive_tests)) OVER (
        PARTITION BY test_type, bulletin_date
        ORDER BY reported_date
    ) AS cumulative_positives,
	sum(delta_positive_tests) AS delta_positive_tests
FROM covid_pr_etl.bioportal_tritemporal_deltas
GROUP BY test_type, bulletin_date, reported_date;


--
-- Same data as `bioportal_tritemporal_deltas`, but aggregated
-- to `received_date`.
--
CREATE TABLE covid_pr_etl.bioportal_received_agg WITH (
    format = 'PARQUET',
    bucketed_by = ARRAY['bulletin_date'],
    bucket_count = 1
) AS
SELECT
	test_type,
	bulletin_date,
	received_date,
	date_diff('day', received_date, bulletin_date)
		AS reported_age,
	sum(tests) AS tests,
	sum(sum(tests)) OVER (
        PARTITION BY test_type, bulletin_date
        ORDER BY received_date
    ) AS cumulative_tests,
	sum(delta_tests) AS delta_tests,
	sum(positive_tests) AS positive_tests,
	sum(sum(positive_tests)) OVER (
        PARTITION BY test_type, bulletin_date
        ORDER BY received_date
    ) AS cumulative_positives,
	sum(delta_positive_tests) AS delta_positive_tests
FROM covid_pr_etl.bioportal_tritemporal_deltas
GROUP BY test_type, bulletin_date, received_date;



----------------------------------------------------------
----------------------------------------------------------
--
-- Views to serve the dashboard.
--

CREATE OR REPLACE VIEW covid_pr_etl.new_daily_cases AS
SELECT
    encounters.bulletin_date,
	encounters.collected_date AS datum_date,
    encounters.rejections,
	nullif(coalesce(bul.confirmed_cases, 0)
    	    + coalesce(bul.probable_cases, 0), 0)
	    AS official,
	encounters.cases AS bioportal,
	bul.deaths AS deaths,
	hosp.previous_day_admission_adult_covid_confirmed
		+ hosp.previous_day_admission_adult_covid_suspected
		+ hosp.previous_day_admission_pediatric_covid_confirmed
		+ hosp.previous_day_admission_pediatric_covid_suspected
		AS hospital_admissions
FROM covid_pr_etl.bioportal_encounters_agg encounters
LEFT OUTER JOIN covid_pr_etl.bulletin_cases bul
	ON bul.bulletin_date = encounters.bulletin_date
	AND bul.datum_date = encounters.collected_date
LEFT OUTER JOIN covid_pr_etl.hhs_hospitals hosp
	ON encounters.collected_date = hosp.date
	AND hosp.date >= DATE '2020-07-28'
ORDER BY encounters.bulletin_date DESC, encounters.collected_date DESC;


CREATE OR REPLACE VIEW covid_pr_etl.molecular_tests_vs_confirmed_cases AS
SELECT
	tests.bulletin_date,
	collected_date,
	cumulative_tests,
	cumulative_confirmed_cases
	    AS cumulative_cases
FROM covid_pr_etl.bioportal_collected_agg tests
INNER JOIN covid_pr_etl.bulletin_cases cases
	ON cases.bulletin_date = tests.bulletin_date
	AND cases.datum_date = tests.collected_date
WHERE tests.bulletin_date > DATE '2020-04-24'
AND test_type = 'Molecular'
ORDER BY tests.bulletin_date DESC, tests.collected_date DESC;


CREATE OR REPLACE VIEW covid_pr_etl.new_daily_tests AS
SELECT
    'Fecha de muestra' AS date_type,
    test_type,
    bulletin_date,
    collected_date AS date,
    tests
FROM covid_pr_etl.bioportal_collected_agg
UNION
SELECT
    'Fecha de reporte' AS date_type,
    test_type,
    bulletin_date,
    reported_date AS date,
    tests
FROM covid_pr_etl.bioportal_reported_agg
ORDER BY bulletin_date DESC, date DESC, test_type, date_type;


--
-- We call this the "naïve" positive rates chart because it uses the
-- simpler, more common metrics that don't account for followup test
-- load.
--
CREATE OR REPLACE VIEW covid_pr_etl.naive_positive_rates AS
SELECT
	bioportal.test_type,
	bioportal.bulletin_date,
	collected_date,
	bioportal.tests,
	bioportal.positive_tests AS positives,
	CASE bioportal.test_type
		WHEN 'Molecular'
		THEN cases.confirmed_cases
	END AS cases
FROM covid_pr_etl.bioportal_collected_agg bioportal
INNER JOIN covid_pr_etl.bulletin_cases cases
	ON cases.bulletin_date = bioportal.bulletin_date
	AND cases.datum_date = bioportal.collected_date
WHERE bioportal.test_type IN ('Molecular', 'Antígeno')
AND bioportal.bulletin_date > DATE '2020-04-24'
AND (
    -- Don't report on antigens earlier than Oct. 24 when
    -- it started in earnest.
	bioportal.test_type != 'Antígeno'
		OR bioportal.collected_date >= DATE '2020-10-24'
)
ORDER BY test_type, bulletin_date DESC, collected_date DESC;


--
-- This is our more sophisticated "positive rate" analysis, which we
-- prefer to call the confirmed vs. rejected cases rate.  The key idea
-- is we don't count followup tests, i.e. test administered to patients
-- that had a positive result in the past three months.
--
CREATE OR REPLACE VIEW covid_pr_etl.confirmed_vs_rejected AS
SELECT
	bulletin_date,
	collected_date,
	initial_positive_molecular AS novels,
	rejections
FROM covid_pr_etl.bioportal_encounters_agg
WHERE bulletin_date > DATE '2020-04-24'
ORDER BY bulletin_date DESC, collected_date DESC;


CREATE OR REPLACE VIEW covid_pr_etl.molecular_lateness_tiers AS
SELECT
	bulletin_date,
	ranges.tier,
	ranges.lo AS tier_order,
	COALESCE(sum(delta_tests) FILTER (
		WHERE delta_tests > 0
	), 0) AS count,
	COALESCE(sum(delta_positive_tests) FILTER (
		WHERE delta_positive_tests > 0
	), 0) AS positive
FROM covid_pr_etl.bioportal_collected_agg
INNER JOIN (VALUES (0, 3, '0-3'),
                   (4, 7, '4-7'),
                   (8, 14, '8-14'),
                   (14, NULL, '> 14')) AS ranges (lo, hi, tier)
	ON ranges.lo <= collected_age
	AND collected_age <= COALESCE(ranges.hi, 2147483647)
WHERE test_type = 'Molecular'
AND bulletin_date >= DATE '2020-07-18'
GROUP BY bulletin_date, ranges.lo, ranges.hi, ranges.tier
ORDER BY bulletin_date DESC, ranges.lo ASC;


CREATE OR REPLACE VIEW covid_pr_etl.recent_daily_cases AS
SELECT
    encounters.bulletin_date,
	encounters.collected_date AS datum_date,
	encounters.encounters AS tests,
    sum(encounters.encounters) OVER (
    	PARTITION BY encounters.bulletin_date
    	ORDER BY encounters.collected_date
    ) cumulative_tests,
	encounters.molecular AS pcr,
    sum(encounters.molecular) OVER (
    	PARTITION BY encounters.bulletin_date
    	ORDER BY encounters.collected_date
    ) cumulative_pcr,
	encounters.antigens AS antigens,
    sum(encounters.antigens) OVER (
    	PARTITION BY encounters.bulletin_date
    	ORDER BY encounters.collected_date
    ) cumulative_antigens,
	encounters.cases,
    sum(encounters.cases) OVER (
    	PARTITION BY encounters.bulletin_date
    	ORDER BY encounters.collected_date
    ) cumulative_cases,
	hosp.previous_day_admission_adult_covid_confirmed
		+ hosp.previous_day_admission_adult_covid_suspected
		+ hosp.previous_day_admission_pediatric_covid_confirmed
		+ hosp.previous_day_admission_pediatric_covid_suspected
		AS admissions,
	sum(hosp.previous_day_admission_adult_covid_confirmed
		+ hosp.previous_day_admission_adult_covid_suspected
		+ hosp.previous_day_admission_pediatric_covid_confirmed
		+ hosp.previous_day_admission_pediatric_covid_suspected) OVER (
    	PARTITION BY encounters.bulletin_date
    	ORDER BY encounters.collected_date
    ) AS cumulative_admissions,
    hosp.inpatient_beds_used_covid,
	bul.deaths AS deaths,
    sum(bul.deaths) OVER (
    	PARTITION BY encounters.bulletin_date
    	ORDER BY encounters.collected_date
    ) cumulative_deaths
FROM covid_pr_etl.bioportal_encounters_agg encounters
LEFT OUTER JOIN covid_pr_etl.bulletin_cases bul
	ON bul.bulletin_date = encounters.bulletin_date
	AND bul.datum_date = encounters.collected_date
LEFT OUTER JOIN covid_pr_etl.hhs_hospitals hosp
	ON encounters.collected_date = hosp.date
	AND hosp.date >= DATE '2020-07-28'
-- We want 42 days of data, so we fetch 56 because we need to
-- calculate a 14-day average 42 days ago:
WHERE encounters.collected_date >= date_add('day', -56, encounters.bulletin_date)
ORDER BY encounters.bulletin_date DESC, encounters.collected_date DESC;


--
-- Lagged case fatality rate, comparing 14-day average of deaths
-- with 14-day average of cases 14 days earlier.
--
CREATE OR REPLACE VIEW covid_pr_etl.lagged_cfr AS
WITH deaths AS (
	SELECT
		bulletin_date,
		datum_date,
		deaths,
		sum(deaths) OVER (
			PARTITION BY bulletin_date
			ORDER BY datum_date
		) AS cumulative_deaths
	FROM covid_pr_etl.bulletin_cases
)
SELECT
	cases.bulletin_date,
	cases.collected_date,
	deaths.datum_date death_date,
	(deaths.cumulative_deaths
		- lag(deaths.cumulative_deaths, 14) OVER (
			PARTITION BY deaths.bulletin_date
			ORDER BY deaths.datum_date
		)) / 14.0
		AS smoothed_deaths,
	(cases.cumulative_cases
		- lag(cases.cumulative_cases, 14) OVER (
			PARTITION BY cases.bulletin_date
			ORDER BY cases.collected_date
		)) / 14.0
		AS smoothed_cases,
	CAST(deaths.cumulative_deaths
		- lag(deaths.cumulative_deaths, 14) OVER (
			PARTITION BY deaths.bulletin_date
			ORDER BY deaths.datum_date
		) AS DOUBLE PRECISION)
		/ (cases.cumulative_cases
			- lag(cases.cumulative_cases, 14) OVER (
				PARTITION BY cases.bulletin_date
				ORDER BY cases.collected_date
			)) AS lagged_cfr
FROM covid_pr_etl.bioportal_curve cases
INNER JOIN deaths
	ON cases.bulletin_date = deaths.bulletin_date
	AND deaths.datum_date = date_add('day', 14, cases.collected_date)
ORDER BY cases.bulletin_date DESC, cases.collected_date DESC;


--
-- COVID-19 hospitalization and ICU occupancy, using HHS data
-- for recent dates, backfilling bad older HHS data with
-- COVID Tracking Project.
--
CREATE OR REPLACE VIEW covid_pr_etl.hospitalizations AS
WITH cutoff AS (
	SELECT DATE '2020-12-07' AS cutoff
)
SELECT
	date,
	hospitalized_currently,
	in_icu_currently
FROM covid_hhs_sources.covid_tracking_hospitalizations
INNER JOIN cutoff
	ON date < cutoff
UNION ALL
SELECT
	date,
	inpatient_beds_used_covid
		AS hospitalized_currently,
	staffed_icu_adult_patients_confirmed_and_suspected_covid
		AS in_icu_currently
FROM covid_pr_etl.hhs_hospitals
INNER JOIN cutoff
	-- Older HHS data is kinda messed up
	ON date >= cutoff
ORDER BY date DESC;


--
-- Hospital bed availabilty and ICU occupancy, using PRDoH data
--
CREATE OR REPLACE VIEW covid_pr_etl.prdoh_hospitalizations AS
SELECT
	fe_hospitalario date,
	'Adultos' age,
	'Camas' resource,
	camas_adultos_total total,
	camas_adultos_covid covid,
	camas_adultos_nocovid nocovid,
	camas_adultos_disp disp
FROM covid19datos_sources.hospitales_daily
UNION ALL
SELECT
	fe_hospitalario date,
	'Adultos' age,
	'UCI' resource,
	camas_icu_total total,
	camas_icu_covid covid,
	camas_icu_nocovid nocovid,
	camas_icu_disp disp
FROM covid19datos_sources.hospitales_daily
UNION ALL
SELECT
	fe_hospitalario date,
	'Pediátricos' age,
	'Camas' resource,
	camas_ped_total total,
	camas_ped_covid covid,
	camas_ped_nocovid nocovid,
	camas_ped_disp disp
FROM covid19datos_sources.hospitales_daily
UNION ALL
SELECT
	fe_hospitalario date,
	'Pediátricos' age,
	'UCI' resource,
	camas_picu_total total,
	camas_picu_covid covid,
	camas_picu_nocovid nocovid,
	camas_picu_disp disp
FROM covid19datos_sources.hospitales_daily
ORDER BY date DESC, age, resource;


--
-- Cases by age group, both as raw numbers and by million population.
-- And when I say million population, I mean using Census Bureau
-- estimate of the population size for that age group.
--
CREATE OR REPLACE VIEW covid_pr_etl.cases_by_age_5y AS
SELECT
	bulletin_date,
	collected_date,
	population,
	age_gte AS youngest,
	age_lt - 1 AS oldest,
	cases,
	1e6 * cases / population
		AS cases_1m,
	deaths,
	1e6 * deaths / population
		AS deaths_1m
FROM covid_pr_etl.bioportal_acs_age_curve
ORDER BY
	bulletin_date DESC,
	collected_date DESC,
	youngest;


--
-- Encounters-based test and case lag.
--
CREATE OR REPLACE VIEW covid_pr_etl.encounter_lag AS
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
FROM covid_pr_etl.bioportal_encounters_agg
WHERE age <= 20
GROUP BY
	bulletin_date,
	CASE
		WHEN 0 <= age AND age < 3 THEN age
		WHEN 3 <= age AND age < 7 THEN 3
		WHEN 7 <= age AND age < 14 THEN 14
		WHEN 14 <= age AND age < 21 THEN 21
	END
ORDER BY bulletin_date DESC, age_lt;
