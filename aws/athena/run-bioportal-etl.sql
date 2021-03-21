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
-- The bitemporal daily bulletin cases table
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
DROP TABLE covid_pr_etl.hhs_hospitals;
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
-- The `orders/basic` row-per-test dataset.
--
CREATE TABLE covid_pr_etl.bioportal_orders_basic WITH (
    format = 'PARQUET',
    bucketed_by = ARRAY['downloaded_date'],
    bucket_count = 1
) AS
WITH downloads AS (
	SELECT
		max(downloadedAt) max_downloaded_at
	FROM covid_pr_sources.orders_basic_parquet_v1
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
	FROM covid_pr_sources.orders_basic_parquet_v1 tests
	INNER JOIN downloads
	    ON downloads.max_downloaded_at = tests.downloadedAt
)
SELECT
    *,
    CASE
	    WHEN test_type IN ('Molecular')
	    THEN CASE
	        WHEN raw_collected_date >= DATE '2020-01-01'
	        THEN raw_collected_date
	        WHEN raw_reported_date >= DATE '2020-03-13'
	        -- Suggested by @rafalab. He uses two days as the value and says
	        -- that's the average, but my spot check says 2.8 days.
	        THEN date_add('day', -3, raw_reported_date)
	        ELSE date_add('day', -3, received_date)
	    END
	    ELSE coalesce(raw_collected_date, raw_reported_date, received_date)
    END AS collected_date,
    CASE
	    WHEN test_type IN ('Molecular')
	    THEN CASE
	        WHEN raw_reported_date >= DATE '2020-03-13'
	        THEN raw_reported_date
	        ELSE received_date
	    END
	    ELSE coalesce(raw_reported_date, raw_collected_date, received_date)
    END AS reported_date
FROM first_clean;


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
-- 2. Flags "followup" tests—tests such that the same patient
--    had a positive test no more than 90 days earlier.
--
-- For #2 we use a three month cutoff following the Council of
-- State and Territorial Epidemiologists (CSTE)'s 2020 Interim
-- Case Definition (Interim-20-ID-02, approved August 5, 2020),
-- which recommends this criterion for distinguishing new cases
-- from existing ones.
--
-- https://wwwn.cdc.gov/nndss/conditions/coronavirus-disease-2019-covid-19/case-definition/2020/08/05/
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
	cur.received_date,
	cur.collected_date,
	cur.reported_date,
	cur.patient_id,
	bool_or(cur.positive) positive,
	-- Note that it's possible for a patient to take both
	-- PCR and antigens on the same date, so `has_molecular`
	-- and `has_antigens` can be both true same day:
	bool_or(cur.test_type = 'Molecular') has_molecular,
	bool_or(cur.test_type = 'Antígeno') has_antigens,
	COALESCE(bool_or(prev.collected_date >= date_add('day', -90, cur.collected_date)
			            AND prev.positive),
             FALSE)
		AS followup
FROM covid_pr_etl.bioportal_orders_basic cur
INNER JOIN downloads
    ON cur.downloaded_at = downloads.max_downloaded_at
    AND cur.downloaded_date = downloads.max_downloaded_date
LEFT OUTER JOIN covid_pr_etl.bioportal_orders_basic prev
	ON prev.downloaded_at = cur.downloaded_at
	AND prev.downloaded_date = cur.downloaded_date
	AND prev.patient_id = cur.patient_id
	AND prev.collected_date < cur.collected_date
WHERE cur.test_type IN ('Molecular', 'Antígeno')
GROUP BY
	cur.downloaded_at,
	cur.downloaded_date,
	cur.received_date,
	cur.collected_date,
	cur.reported_date,
	cur.patient_id;


----------------------------------------------------------
----------------------------------------------------------
--
-- Aggregates off which we run most of our analyses.
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
GROUP BY test_type, bulletins.bulletin_date, collected_date, reported_date;


CREATE TABLE covid_pr_etl.bioportal_tritemporal_deltas WITH (
    format = 'PARQUET',
    bucketed_by = ARRAY['bulletin_date'],
    bucket_count = 1
) AS
SELECT
	test_type,
	bulletin_date,
	reported_date,
	collected_date,
	tests,
	tests - COALESCE(lag(tests) OVER (
        PARTITION BY test_type, collected_date, reported_date
	    ORDER BY bulletin_date
    ), 0) AS delta_tests,
	positive_tests,
	positive_tests - COALESCE(lag(positive_tests) OVER (
        PARTITION BY test_type, collected_date, reported_date
	    ORDER BY bulletin_date
    ), 0) AS delta_positive_tests
FROM covid_pr_etl.bioportal_tritemporal_counts
WHERE collected_date <= bulletin_date
AND reported_date <= bulletin_date;


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
-- Encounters cube
--
CREATE TABLE covid_pr_etl.bioportal_encounters_agg WITH (
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
	bulletins.bulletin_date,
	tests.collected_date,
	tests.received_date,
	count(*) AS encounters,
	count(*) FILTER (
		WHERE tests.positive
		AND NOT tests.followup
	) cases,
	count(*) FILTER (
		WHERE tests.has_molecular
		AND NOT tests.positive
		AND NOT tests.followup
	) rejections,
	-- Note that `has_antigens` and `has_molecular` don't
	-- have to add up to `encounters` because a person may
	-- get both test types the same day. Similar remarks
	-- apply to many of the sums below.
	count(*) FILTER (
		WHERE tests.has_antigens
	) has_antigens,
	count(*) FILTER (
		WHERE tests.has_molecular
	) has_molecular,
	count(*) FILTER (
		WHERE NOT tests.followup
		AND tests.has_molecular
	) AS initial_molecular,
	count(*) FILTER (
		WHERE NOT tests.followup
		AND tests.has_molecular
		AND tests.positive
	) AS initial_molecular_positives
FROM covid_pr_etl.bioportal_encounters tests
INNER JOIN downloads
	ON tests.downloaded_at = downloads.max_downloaded_at
INNER JOIN bulletins
	ON bulletins.bulletin_date < downloads.max_downloaded_date
	AND tests.received_date <= bulletins.bulletin_date
AND DATE '2020-03-01' <= tests.collected_date
AND tests.collected_date <= tests.received_date
AND DATE '2020-03-01' <= tests.reported_date
AND tests.reported_date <= tests.received_date
GROUP BY
	bulletins.bulletin_date,
	tests.collected_date,
	tests.received_date
ORDER BY
	bulletins.bulletin_date,
	tests.collected_date,
	tests.received_date;


CREATE TABLE covid_pr_etl.bioportal_encounters_collected_agg WITH (
    format = 'PARQUET',
    bucketed_by = ARRAY['bulletin_date'],
    bucket_count = 1
) AS
SELECT
	bulletin_date,
	collected_date,
	sum(encounters) AS encounters,
	sum(cases) AS cases,
	sum(rejections) AS rejections,
	sum(has_antigens) AS has_antigens,
	sum(has_molecular) AS has_molecular,
	sum(initial_molecular) AS initial_molecular,
	sum(initial_molecular_positives) AS initial_molecular_positives
FROM covid_pr_etl.bioportal_encounters_agg
GROUP BY bulletin_date, collected_date
ORDER BY bulletin_date, collected_date;



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
	sum(cases) cases,
    sum(sum(cases)) OVER (
		PARTITION BY bulletin_date
		ORDER BY collected_date
	) AS cumulative_cases,
	sum(cases) - coalesce(lag(sum(cases)) OVER (
		PARTITION BY collected_date
		ORDER BY bulletin_date
	), 0) AS delta_cases
FROM covid_pr_etl.bioportal_encounters_agg
GROUP BY bulletin_date, collected_date
ORDER BY bulletin_date DESC, collected_date DESC;


----------------------------------------------------------
----------------------------------------------------------
--
-- Views to serve the dashboard.
--

CREATE OR REPLACE VIEW covid_pr_etl.new_daily_cases AS
SELECT
    bio.bulletin_date,
	bio.collected_date AS datum_date,
    encounters.rejections,
	nullif(coalesce(bul.confirmed_cases, 0)
    	    + coalesce(bul.probable_cases, 0), 0)
	    AS official,
	bio.cases AS bioportal,
	bul.deaths AS deaths,
	hosp.previous_day_admission_adult_covid_confirmed
		+ hosp.previous_day_admission_adult_covid_suspected
		+ hosp.previous_day_admission_pediatric_covid_confirmed
		+ hosp.previous_day_admission_pediatric_covid_suspected
		AS hospital_admissions
FROM covid_pr_etl.bioportal_curve bio
INNER JOIN covid_pr_etl.bioportal_encounters_collected_agg encounters
	ON encounters.bulletin_date = bio.bulletin_date
	AND encounters.collected_date = bio.collected_date
LEFT OUTER JOIN covid_pr_etl.bulletin_cases bul
	ON bul.bulletin_date = bio.bulletin_date
	AND bul.datum_date = bio.collected_date
LEFT OUTER JOIN covid_pr_etl.hhs_hospitals hosp
	ON bio.collected_date = hosp.date
	AND hosp.date >= DATE '2020-07-28'
ORDER BY bio.bulletin_date DESC, bio.collected_date DESC;


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
-- We don't use this one in the dashboard but we keep it around because
-- we're sometimes curious to know.
--
CREATE OR REPLACE VIEW covid_pr_etl.naive_serological_positive_rates AS
SELECT
	serological.test_type,
	serological.bulletin_date,
	collected_date,
	(serological.cumulative_tests - lag(serological.cumulative_tests, 7) OVER (
		PARTITION BY serological.test_type, serological.bulletin_date
		ORDER BY collected_date
	)) / 7.0 AS smoothed_daily_tests,
	(serological.cumulative_positives - lag(serological.cumulative_positives, 7) OVER (
		PARTITION BY serological.test_type, serological.bulletin_date
		ORDER BY collected_date
	)) / 7.0 AS smoothed_daily_positive_tests,
	(cases.cumulative_confirmed_cases - lag(cases.cumulative_confirmed_cases, 7) OVER (
		PARTITION BY serological.test_type, serological.bulletin_date
		ORDER BY collected_date
	)) / 7.0 AS smoothed_daily_cases
FROM covid_pr_etl.bioportal_collected_agg serological
INNER JOIN covid_pr_etl.bulletin_cases cases
	ON cases.bulletin_date = serological.bulletin_date
	AND cases.datum_date = serological.collected_date
WHERE serological.test_type = 'Serológica'
AND serological.bulletin_date > DATE '2020-04-24'
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
	initial_molecular_positives AS novels,
	rejections
FROM covid_pr_etl.bioportal_encounters_collected_agg
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
WITH tests AS (
	SELECT
		bulletin_date,
		collected_date,
		sum(tests) AS tests,
		sum(tests) FILTER (WHERE test_type = 'Molecular')
			AS pcr,
		sum(tests) FILTER (WHERE test_type = 'Antígeno')
			AS antigens
	FROM covid_pr_etl.bioportal_collected_agg
	WHERE test_type IN ('Molecular', 'Antígeno')
    -- We want 42 days of data, so we fetch 56 because we need to
    -- calculate a 14-day average 42 days ago:
	AND collected_date >= date_add('day', -56, bulletin_date)
	GROUP BY bulletin_date, collected_date
)
SELECT
    cases.bulletin_date,
    cases.datum_date,
    tests.tests,
    sum(tests.tests) OVER (
    	PARTITION BY cases.bulletin_date
    	ORDER BY cases.datum_date
    ) cumulative_tests,
    tests.pcr,
    sum(tests.pcr) OVER (
    	PARTITION BY cases.bulletin_date
    	ORDER BY cases.datum_date
    ) cumulative_pcr,
    tests.antigens,
    sum(tests.antigens) OVER (
    	PARTITION BY cases.bulletin_date
    	ORDER BY cases.datum_date
    ) cumulative_antigens,
	cases.bioportal AS cases,
    sum(cases.bioportal) OVER (
    	PARTITION BY cases.bulletin_date
    	ORDER BY cases.datum_date
    ) cumulative_cases,
    cases.hospital_admissions AS admissions,
    sum(cases.hospital_admissions) OVER (
    	PARTITION BY cases.bulletin_date
    	ORDER BY cases.datum_date
    ) cumulative_admissions,
	cases.deaths,
    sum(cases.deaths) OVER (
    	PARTITION BY cases.bulletin_date
    	ORDER BY cases.datum_date
    ) cumulative_deaths
FROM covid_pr_etl.new_daily_cases cases
INNER JOIN tests
	ON cases.bulletin_date = tests.bulletin_date
	AND cases.datum_date = tests.collected_date
-- We want 42 days of data, so we fetch 56 because we need to
-- calculate a 14-day average 42 days ago:
WHERE cases.datum_date >= date_add('day', -56, cases.bulletin_date)
ORDER BY bulletin_date DESC, datum_date DESC;


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
