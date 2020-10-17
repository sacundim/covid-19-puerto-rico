----------------------------------------------------------
----------------------------------------------------------
--
-- Rebuild the whole schema from scratch from the raw CSV tables.
--


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
-- The `minimal-info-unique-tests` row-per-test dataset.
--
DROP TABLE IF EXISTS covid_pr_etl.bioportal_tests;
CREATE TABLE covid_pr_etl.bioportal_tests WITH (
    format = 'PARQUET',
    bucketed_by = ARRAY['bulletin_date'],
    bucket_count = 4
) AS
WITH tests_csv_union AS (
    SELECT
        downloadedAt,
        collectedDate,
        reportedDate,
        ageRange,
        testType,
        result,
        patientCity,
        createdAt
    FROM covid_pr_sources.tests_csv_v1
    UNION ALL
    SELECT
        downloadedAt,
        collectedDate,
        reportedDate,
        ageRange,
        testType,
        result,
        patientCity,
        createdAt
    FROM covid_pr_sources.tests_csv_v2
), first_clean AS (
    SELECT
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
        CASE patientCity
            WHEN '' THEN NULL
            WHEN 'Loiza' THEN 'Loíza'
            WHEN 'Rio Grande' THEN 'Río Grande'
            ELSE patientCity
        END AS municipality,
        testType AS test_type,
        result,
        COALESCE(result, '') LIKE '%Positive%' AS positive
    FROM tests_csv_union
)
SELECT
    *,
    CASE
        WHEN raw_collected_date >= DATE '2020-01-01'
        THEN raw_collected_date
        WHEN raw_reported_date >= DATE '2020-03-13'
        -- Suggested by @rafalab. He uses two days as the value and says
        -- that's the average, but my spot check says 2.8 days.
        THEN raw_reported_date - INTERVAL '3' DAY
        ELSE date(created_at - INTERVAL '4' HOUR) - INTERVAL '3' DAY
    END AS collected_date,
    CASE
        WHEN raw_reported_date >= DATE '2020-03-13'
        THEN raw_reported_date
        ELSE date(created_at - INTERVAL '4' HOUR)
    END AS reported_date
FROM first_clean;

--
-- The `minimal-info` row-per-test dataset. This one differs from
-- `minimal-info-unique-tests` in that:
--
-- 1. It has a `patient_id` field (which makes it much bigger in disk space);
-- 2. It has `region` instead of `municipality`.
--
-- If we had longer history of this one we might abandon `minimal-info-unique-tests`
-- for this one.
--
DROP TABLE IF EXISTS covid_pr_etl.bioportal_cases;
CREATE TABLE covid_pr_etl.bioportal_cases WITH (
    format = 'PARQUET',
    bucketed_by = ARRAY['bulletin_date'],
    bucket_count = 6
) AS
WITH first_clean AS (
	SELECT
	    CAST(from_iso8601_timestamp(downloadedAt) AS TIMESTAMP)
	        AS downloaded_at,
	    CAST(from_iso8601_timestamp(downloadedAt) AS DATE) - INTERVAL '1' DAY
	        AS bulletin_date,
	    CAST(date_parse(nullif(collectedDate, ''), '%m/%d/%Y') AS DATE)
	        AS raw_collected_date,
	    CAST(date_parse(nullif(reportedDate, ''), '%m/%d/%Y') AS DATE)
	        AS raw_reported_date,
	    date_parse(createdAt, '%m/%d/%Y %H:%i') AS created_at,
	    from_hex(replace(nullif(patientId, ''), '-')) AS patient_id,
	    nullif(ageRange, '') AS age_range,
	    nullif(region, '') AS region,
	    testType AS test_type,
	    result,
	    COALESCE(result, '') LIKE '%Positive%' AS positive
	FROM covid_pr_sources.cases_csv_v1
)
SELECT
    *,
    CASE
        WHEN raw_collected_date >= DATE '2020-01-01'
        THEN raw_collected_date
        WHEN raw_reported_date >= DATE '2020-03-13'
        -- Suggested by @rafalab. He uses two days as the value and says
        -- that's the average, but my spot check says 2.8 days.
        THEN raw_reported_date - INTERVAL '3' DAY
        ELSE date(created_at - INTERVAL '4' HOUR) - INTERVAL '3' DAY
    END AS collected_date,
    CASE
        WHEN raw_reported_date >= DATE '2020-03-13'
        THEN raw_reported_date
        ELSE date(created_at - INTERVAL '4' HOUR)
    END AS reported_date
FROM first_clean;


--
-- This is data set computed off `bioportal_cases` is our
-- analysis of which tests are likely to be followups of
-- earlier positive tests.
--
-- We classify a test as a "followup" if the same `patient_id`
-- has an earlier positive test that was collected within three
-- months.  We use a three month cutoff following the Council of
-- State and Territorial Epidemiologists (CSTE)'s 2020 Interim
-- Case Definition (Interim-20-ID-02, approved August 5, 2020),
-- which recommends this criterion for distinguishing new cases
-- from existing ones.
--
-- https://wwwn.cdc.gov/nndss/conditions/coronavirus-disease-2019-covid-19/case-definition/2020/08/05/
DROP TABLE IF EXISTS covid_pr_etl.bioportal_followups;
CREATE TABLE covid_pr_etl.bioportal_followups WITH (
    format = 'PARQUET',
    bucketed_by = ARRAY['bulletin_date'],
    bucket_count = 1
) AS
SELECT
	cur.test_type,
	cur.bulletin_date,
	cur.raw_collected_date,
	cur.raw_reported_date,
	cur.collected_date,
	cur.reported_date,
	cur.positive,
	COALESCE(bool_or(prev.raw_collected_date >=
				date_add('day', -90, cur.raw_collected_date)
			AND prev.positive), FALSE)
		AS followup
FROM covid_pr_etl.bioportal_cases cur
LEFT OUTER JOIN covid_pr_etl.bioportal_cases prev
	ON prev.test_type = cur.test_type
	AND prev.bulletin_date = cur.bulletin_date
	AND prev.patient_id = cur.patient_id
	AND prev.raw_collected_date < cur.raw_collected_date
GROUP BY
	cur.test_type,
	cur.bulletin_date,
	cur.raw_collected_date,
	cur.raw_reported_date,
	cur.collected_date,
	cur.reported_date,
	cur.patient_id,
	cur.positive;


----------------------------------------------------------
----------------------------------------------------------
--
-- Aggregates off which we run most of our analyses.
--

DROP TABLE IF EXISTS covid_pr_etl.bioportal_tritemporal_counts;
CREATE TABLE covid_pr_etl.bioportal_tritemporal_counts WITH (
    format = 'PARQUET',
    bucketed_by = ARRAY['bulletin_date'],
    bucket_count = 1
) AS
SELECT
	test_type,
	bulletin_date,
	reported_date,
	collected_date,
	count(*) tests,
	count(*) FILTER (WHERE positive)
		AS positive_tests
FROM covid_pr_etl.bioportal_tests
WHERE DATE '2020-03-01' <= collected_date
AND collected_date <= bulletin_date
AND DATE '2020-03-01' <= reported_date
AND reported_date <= bulletin_date
GROUP BY test_type, bulletin_date, collected_date, reported_date;


DROP TABLE IF EXISTS covid_pr_etl.bioportal_tritemporal_deltas;
CREATE TABLE covid_pr_etl.bioportal_tritemporal_deltas AS
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


DROP TABLE IF EXISTS covid_pr_etl.bioportal_collected_agg;
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


DROP TABLE IF EXISTS covid_pr_etl.bioportal_reported_agg;
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


DROP TABLE IF EXISTS covid_pr_etl.bioportal_followups_collected_agg;
CREATE TABLE covid_pr_etl.bioportal_followups_collected_agg WITH (
    format = 'PARQUET',
    bucketed_by = ARRAY['bulletin_date'],
    bucket_count = 1
) AS
WITH dailies AS (
	SELECT
		test_type,
		bulletin_date,
		collected_date,
		count(*) tests,
		count(*) FILTER (WHERE positive)
			AS positives,
		count(*) FILTER (WHERE positive AND NOT followup)
			AS novels,
		count(*) FILTER (WHERE NOT positive AND NOT followup)
			AS rejections,
		count(*) FILTER (WHERE followup)
			AS followups
	FROM covid_pr_etl.bioportal_followups
	WHERE DATE '2020-03-01' <= collected_date
	AND collected_date <= bulletin_date
	AND DATE '2020-03-01' <= reported_date
	AND reported_date <= bulletin_date
	GROUP BY test_type, bulletin_date, collected_date
)
SELECT
	*,
	sum(tests) OVER (
		PARTITION BY test_type,  bulletin_date
		ORDER BY collected_date
	) AS cumulative_tests,
	sum(positives) OVER (
		PARTITION BY test_type,  bulletin_date
		ORDER BY collected_date
	) AS cumulative_positives,
	sum(novels) OVER (
		PARTITION BY test_type,  bulletin_date
		ORDER BY collected_date
	) AS cumulative_novels,
	sum(rejections) OVER (
		PARTITION BY test_type,  bulletin_date
		ORDER BY collected_date
	) AS cumulative_rejections,
	sum(followups) OVER (
		PARTITION BY test_type,  bulletin_date
		ORDER BY collected_date
	) AS cumulative_followups
FROM dailies;


----------------------------------------------------------
----------------------------------------------------------
--
-- Views to serve the dashboard.
--

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
    (cumulative_tests - lag(cumulative_tests, 7) OVER (
		PARTITION BY test_type, bulletin_date
		ORDER BY collected_date
	)) / 7.0 AS smoothed_daily_tests
FROM covid_pr_etl.bioportal_collected_agg
UNION
SELECT
    'Fecha de reporte' AS date_type,
    test_type,
    bulletin_date,
    reported_date AS date,
    (cumulative_tests - lag(cumulative_tests, 7) OVER (
		PARTITION BY test_type, bulletin_date
		ORDER BY reported_date
	)) / 7.0 AS smoothed_daily_tests
FROM covid_pr_etl.bioportal_reported_agg
ORDER BY bulletin_date DESC, date DESC, test_type, date_type;


--
-- We call this the "naïve" positive rates chart because it uses the 
-- simpler, more common metrics that don't account for followup test
-- load.
-- 
CREATE OR REPLACE VIEW covid_pr_etl.naive_positive_rates AS
SELECT
	molecular.test_type,
	molecular.bulletin_date,
	collected_date,
	(molecular.cumulative_tests - lag(molecular.cumulative_tests, 7) OVER (
		PARTITION BY molecular.test_type, molecular.bulletin_date
		ORDER BY collected_date
	)) / 7.0 AS smoothed_daily_tests,
	(molecular.cumulative_positives - lag(molecular.cumulative_positives, 7) OVER (
		PARTITION BY molecular.test_type, molecular.bulletin_date
		ORDER BY collected_date
	)) / 7.0 AS smoothed_daily_positives,
	(cases.cumulative_confirmed_cases - lag(cases.cumulative_confirmed_cases, 7) OVER (
		PARTITION BY molecular.test_type, molecular.bulletin_date
		ORDER BY collected_date
	)) / 7.0 AS smoothed_daily_cases
FROM covid_pr_etl.bioportal_collected_agg molecular
INNER JOIN covid_pr_etl.bulletin_cases cases
	ON cases.bulletin_date = molecular.bulletin_date
	AND cases.datum_date = molecular.collected_date
WHERE molecular.test_type = 'Molecular'
AND molecular.bulletin_date > DATE '2020-04-24'
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
WHERE serological.test_type = 'Serological'
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
	molecular.test_type,
	molecular.bulletin_date,
	collected_date,
	molecular.tests,
	molecular.positives,
	molecular.novels,
	molecular.rejections,
	cases.confirmed_cases AS cases
FROM covid_pr_etl.bioportal_followups_collected_agg molecular
INNER JOIN covid_pr_etl.bulletin_cases cases
	ON cases.bulletin_date = molecular.bulletin_date
	AND cases.datum_date = molecular.collected_date
WHERE molecular.test_type = 'Molecular'
AND molecular.bulletin_date > DATE '2020-04-24'
ORDER BY test_type, bulletin_date DESC, collected_date DESC;


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


CREATE OR REPLACE VIEW covid_pr_etl.tests_received AS
SELECT
	test_type,
	bulletin_date,
	sum(delta_tests) FILTER (WHERE collected_age <= 14)
		AS recent_tests,
	sum(delta_positive_tests) FILTER (WHERE collected_age <= 14)
		AS recent_positive_tests,
	sum(delta_tests) FILTER (WHERE collected_age > 14)
		AS late_tests,
	sum(delta_positive_tests) FILTER (WHERE collected_age > 14)
		AS late_positive_tests
FROM covid_pr_etl.bioportal_collected_agg
GROUP BY test_type, bulletin_date
ORDER BY bulletin_date DESC, test_type DESC;


CREATE OR REPLACE VIEW covid_pr_etl.unsmoothed_tests AS
SELECT
    bulletin_date,
	collected_date,
	tests,
	positive_tests,
	100.0 * positive_tests
		/ tests
		AS positive_rate
FROM covid_pr_etl.bioportal_collected_agg
ORDER BY bulletin_date DESC, collected_date DESC;