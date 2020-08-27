CREATE TEMPORARY TABLE bioportal_raw (
    downloadedAt TIMESTAMP WITHOUT TIME ZONE,
    patientId UUID,
    collectedDate TEXT,
    reportedDate TEXT,
    ageRange TEXT,
    testType TEXT,
    result TEXT,
    patientCity TEXT,
    createdAt TEXT
);


COPY bioportal_raw (
    downloadedAt, collectedDate, reportedDate, ageRange, testType, result, patientCity, createdAt
)
FROM PROGRAM 'for file in $(ls /data/bioportal/v1/minimal-info-unique-tests_V1_*.csv.bz2); do (cat "${file}" |bunzip2 |tail -n+2); done'
    CSV ENCODING 'UTF-8' NULL '';

COPY bioportal_raw (
    downloadedAt, patientId, collectedDate, reportedDate, ageRange, testType, result, patientCity, createdAt
)
FROM PROGRAM 'for file in $(ls /data/bioportal/v2/minimal-info-unique-tests_V2_*.csv.bz2); do (cat "${file}" |bunzip2 |tail -n+2); done'
    CSV ENCODING 'UTF-8' NULL '';


INSERT INTO bioportal_tests (
    downloaded_at, raw_collected_date, raw_reported_date, created_at,
    patient_id, age_range, municipality, test_type, result
)
SELECT
    downloadedAt AS downloaded_at,
    to_date(collectedDate, 'MM/DD/YYYY') AS raw_collected_date,
    to_date(reportedDate, 'MM/DD/YYYY') AS raw_reported_date,
    to_timestamp(createdAt, 'MM/DD/YYYY HH24:MI') AS created_at,
    patientId AS patient_id,
    ageRange AS age_range,
    CASE patientCity
    WHEN 'Rio Grande' THEN 'Río Grande'
    ELSE patientCity
    END AS municipality,
    testType AS test_type,
    result
FROM bioportal_raw;

SET maintenance_work_mem='1GB';
CREATE INDEX ON bioportal_tests (downloaded_at, test_type, reported_date, collected_date, positive);
CREATE INDEX ON bioportal_tests (downloaded_at, test_type, collected_date, reported_date, positive);

ANALYZE VERBOSE bioportal_tests;