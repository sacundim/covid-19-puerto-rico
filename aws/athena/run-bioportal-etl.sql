----------------------------------------------------------
----------------------------------------------------------
--
-- Rebuild the whole schema from scratch from the raw CSV tables.
--

DROP VIEW IF EXISTS covid_pr_etl.molecular_lateness_tiers;
DROP VIEW IF EXISTS covid_pr_etl.testing_load;
DROP VIEW IF EXISTS covid_pr_etl.new_daily_tests;
DROP VIEW IF EXISTS covid_pr_etl.positive_rates;
DROP VIEW IF EXISTS covid_pr_etl.molecular_tests_vs_confirmed_cases;

DROP TABLE IF EXISTS covid_pr_etl.bioportal_tritemporal_counts;
DROP TABLE IF EXISTS covid_pr_etl.bioportal_tritemporal_deltas;
DROP TABLE IF EXISTS covid_pr_etl.bioportal_collected_agg;
DROP TABLE IF EXISTS covid_pr_etl.bioportal_reported_agg;

DROP TABLE IF EXISTS covid_pr_etl.bulletin_cases;
DROP TABLE IF EXISTS covid_pr_etl.bioportal_tests;


----------------------------------------------------------
----------------------------------------------------------
--
-- The big core tables with disaggregated clean data.
--

CREATE TABLE covid_pr_etl.bulletin_cases AS
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

CREATE TABLE covid_pr_etl.bioportal_tests AS
WITH tests_csv_union AS (
    SELECT
        downloadedAt,
        '' AS patientId,
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
        patientId,
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
        nullif(patientId, '') AS patient_id,
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


----------------------------------------------------------
----------------------------------------------------------
--
-- Aggregates off which we run most of our analyses.
--

CREATE TABLE covid_pr_etl.bioportal_tritemporal_counts AS
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


CREATE TABLE covid_pr_etl.bioportal_collected_agg AS
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
    ) AS cumulative_positive_tests,
	sum(delta_positive_tests) AS delta_positive_tests
FROM covid_pr_etl.bioportal_tritemporal_deltas
GROUP BY test_type, bulletin_date, collected_date;

CREATE TABLE covid_pr_etl.bioportal_reported_agg AS
SELECT
	test_type,
	bulletin_date,
	reported_date,
	date_diff('day', reported_date, bulletin_date)
		AS collected_age,
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
    ) AS cumulative_positive_tests,
	sum(delta_positive_tests) AS delta_positive_tests
FROM covid_pr_etl.bioportal_tritemporal_deltas
GROUP BY test_type, bulletin_date, reported_date;


----------------------------------------------------------
----------------------------------------------------------
--
-- Views to serve the dashboard.
--

CREATE VIEW covid_pr_etl.molecular_tests_vs_confirmed_cases AS
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


CREATE VIEW covid_pr_etl.new_daily_tests AS
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


CREATE VIEW covid_pr_etl.positive_rates AS
SELECT
	molecular.test_type,
	molecular.bulletin_date,
	collected_date,
	(molecular.cumulative_tests - lag(molecular.cumulative_tests, 7) OVER (
		PARTITION BY molecular.test_type, molecular.bulletin_date
		ORDER BY collected_date
	)) / 7.0 AS smoothed_daily_tests,
	(molecular.cumulative_positive_tests - lag(molecular.cumulative_positive_tests, 7) OVER (
		PARTITION BY molecular.test_type, molecular.bulletin_date
		ORDER BY collected_date
	)) / 7.0 AS smoothed_daily_positive_tests,
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
UNION ALL
SELECT
	serological.test_type,
	serological.bulletin_date,
	collected_date,
	(serological.cumulative_tests - lag(serological.cumulative_tests, 7) OVER (
		PARTITION BY serological.test_type, serological.bulletin_date
		ORDER BY collected_date
	)) / 7.0 AS smoothed_daily_tests,
	(serological.cumulative_positive_tests - lag(serological.cumulative_positive_tests, 7) OVER (
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


CREATE VIEW covid_pr_etl.testing_load AS
SELECT
	bca.bulletin_date,
	bca.collected_date,
	(bca.cumulative_tests - lag(bca.cumulative_tests, 7) OVER (
		PARTITION BY test_type, bca.bulletin_date
		ORDER BY collected_date
	)) / 7.0 AS tests,
	((bca.cumulative_positive_tests - lag(bca.cumulative_positive_tests, 7) OVER (
		PARTITION BY test_type, bca.bulletin_date
		ORDER BY collected_date
	)) - COALESCE(b.cumulative_confirmed_cases - lag(b.cumulative_confirmed_cases, 7) OVER (
		PARTITION BY test_type, bca.bulletin_date
		ORDER BY collected_date
	), 0)) / 7.0 AS duplicate_positives
FROM covid_pr_etl.bioportal_collected_agg bca
INNER JOIN covid_pr_etl.bulletin_cases b
	ON b.bulletin_date = bca.bulletin_date
	AND b.datum_date = bca.collected_date
WHERE test_type = 'Molecular'
ORDER BY bca.bulletin_date DESC, bca.collected_date DESC;

CREATE VIEW covid_pr_etl.molecular_lateness_tiers AS
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
