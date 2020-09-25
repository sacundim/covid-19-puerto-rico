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

CREATE VIEW tests_csv_union AS
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
WHERE downloadedAt != 'downloadedAt';

CREATE VIEW tests_union_scalar_clean AS
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
FROM tests_csv_union;


CREATE TABLE bioportal_tests WITH (
    external_location = 's3://covid-19-puerto-rico/bioportal/tests/'
) AS
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
FROM tests_union_scalar_clean;

CREATE TABLE bioportal_tritemporal_counts WITH (
    external_location = 's3://covid-19-puerto-rico/bioportal/bioportal_tritemporal_counts/'
) AS
SELECT
	test_type,
	bulletin_date,
	reported_date,
	collected_date,
	count(*) tests,
	count(*) FILTER (WHERE positive)
		AS positive_tests
FROM bioportal_tests
WHERE DATE '2020-03-01' <= collected_date
AND collected_date <= bulletin_date
AND DATE '2020-03-01' <= reported_date
AND reported_date <= bulletin_date
GROUP BY test_type, bulletin_date, collected_date, reported_date;

CREATE TABLE bioportal_tritemporal_deltas WITH (
    external_location = 's3://covid-19-puerto-rico/bioportal/bioportal_tritemporal_deltas/'
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
FROM bioportal_tritemporal_counts
WHERE collected_date <= bulletin_date
AND reported_date <= bulletin_date;

CREATE TABLE bioportal_collected_agg WITH (
    external_location = 's3://covid-19-puerto-rico/bioportal/bioportal_tritemporal_deltas/'
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
    ) AS cumulative_positive_tests,
	sum(delta_positive_tests) AS delta_positive_tests
FROM bioportal_tritemporal_deltas
GROUP BY test_type, bulletin_date, collected_date;

CREATE TABLE bioportal_reported_agg WITH (
    external_location = 's3://covid-19-puerto-rico/bioportal/bioportal_reported_agg/'
) AS
SELECT
	test_type,
	bulletin_date,
	reported_date,
	date_diff('day', collected_date, bulletin_date)
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
FROM bioportal_tritemporal_deltas
GROUP BY test_type, bulletin_date, reported_date
WINDOW cumulative AS (
	PARTITION BY test_type, bulletin_date
	ORDER BY reported_date
);


CREATE VIEW new_daily_tests AS
SELECT
    'Fecha de muestra' AS date_type,
    test_type,
    bulletin_date,
    collected_date AS date,
    tests
FROM bioportal_collected_agg
UNION
SELECT
    'Fecha de reporte' AS date_type,
    test_type,
    bulletin_date,
    reported_date AS date,
    tests
FROM bioportal_reported_agg;
