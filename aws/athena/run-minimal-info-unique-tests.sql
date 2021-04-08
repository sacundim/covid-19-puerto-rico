--
-- The `minimal-info-unique-tests` row-per-test dataset.
-- We only run this occasionally for ad-hoc analyses.
--
DROP TABLE IF EXISTS covid_pr_etl.bioportal_tests;
CREATE TABLE covid_pr_etl.bioportal_tests WITH (
    format = 'PARQUET'
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
    FROM covid_pr_sources.minimal_info_unique_tests_parquet_v3
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
            THEN raw_reported_date - INTERVAL '3' DAY
            ELSE date(created_at - INTERVAL '4' HOUR) - INTERVAL '3' DAY
        END
        ELSE coalesce(raw_collected_date, raw_reported_date)
    END AS collected_date,
    CASE
        WHEN test_type IN ('Molecular')
        THEN CASE
            WHEN raw_reported_date >= DATE '2020-03-13'
            THEN raw_reported_date
            ELSE date(created_at - INTERVAL '4' HOUR)
        END
        ELSE coalesce(raw_reported_date, raw_collected_date)
    END AS reported_date
FROM first_clean;