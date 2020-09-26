--
-- Create the basic schema for Bioportal data.
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


SELECT
    bulletin_date,
    datum_date,
    bulletin_date - datum_date AS age,

    confirmed_cases,
    sum(confirmed_cases) OVER bulletin
        AS cumulative_confirmed_cases,
    COALESCE(confirmed_cases, 0)
        - COALESCE(lag(confirmed_cases) OVER datum, 0)
        AS delta_confirmed_cases,

    probable_cases,
    sum(probable_cases) OVER bulletin
        AS cumulative_probable_cases,
    COALESCE(probable_cases, 0)
        - COALESCE(lag(probable_cases) OVER datum, 0)
        AS delta_probable_cases,

    deaths,
    sum(deaths) OVER bulletin
        AS cumulative_deaths,
    COALESCE(deaths, 0)
        - COALESCE(lag(deaths) OVER datum, 0)
        AS delta_deaths
FROM bitemporal
WINDOW bulletin AS (
	PARTITION BY bulletin_date
	ORDER BY datum_date
), datum AS (
	PARTITION BY datum_date
	ORDER BY bulletin_date
	RANGE '1 day' PRECEDING
)
