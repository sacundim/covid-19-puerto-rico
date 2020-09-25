--
-- Create the basic schema for Bioportal data.
--

CREATE EXTERNAL TABLE tests_csv_v1 (
    downloadedAt STRING,
    collectedDate STRING,
    reportedDate STRING,
    ageRange STRING,
    testType STRING,
    result STRING,
    patientCity STRING,
    createdAt STRING
) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
LOCATION 's3://covid-19-puerto-rico/bioportal-archive/tests/csv_v1/';

CREATE EXTERNAL TABLE tests_csv_v2 (
    downloadedAt STRING,
    patientId STRING,
    collectedDate STRING,
    reportedDate STRING,
    ageRange STRING,
    testType STRING,
    result STRING,
    patientCity STRING,
    createdAt STRING
) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
LOCATION 's3://covid-19-puerto-rico/bioportal-archive/tests/csv_v2/';


CREATE TABLE bioportal_tests WITH (
    external_location = 's3://covid-19-puerto-rico/bioportal/bioportal_tests/'
) AS
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
    FROM tests_csv_v1
    -- Hive stuff is profoundly stupid. It doesn't know how to skip the header row.
    WHERE downloadedAt != 'downloadedAt'
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
    FROM tests_csv_v2
    WHERE downloadedAt != 'downloadedAt'
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
