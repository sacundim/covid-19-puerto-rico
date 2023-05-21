--
-- Create the schema that reads from the Biostatistics source data files:
--
-- * https://biostatistics.salud.pr.gov/swagger/index.html
--
-- We make zero effort here to clean these files or even
-- to parse the data types and treat them as strings unless
-- it's zero effort to coerce them to right type.
--

DROP DATABASE IF EXISTS biostatistics_sources CASCADE;

CREATE DATABASE biostatistics_sources
LOCATION 's3://covid-19-puerto-rico-data/biostatistics.salud.pr.gov/';



--------------------------------------------------------------------------
--------------------------------------------------------------------------
--
-- Data Sources
--

CREATE EXTERNAL TABLE biostatistics_sources.data_sources_parquet_v1 (
    id STRING,
    name STRING,
    recordCount BIGINT,
    lastUpdated STRING,
    originTimeZone STRING
) PARTITIONED BY (downloaded_date STRING)
STORED AS PARQUET
LOCATION 's3://covid-19-puerto-rico-data/biostatistics.salud.pr.gov/data-sources/parquet_v1/';

CREATE EXTERNAL TABLE biostatistics_sources.data_sources_parquet_v2 (
    id STRING,
    name STRING,
    recordCount BIGINT,
    lastUpdated TIMESTAMP,
    originTimeZone STRING,
    downloadedAt TIMESTAMP
) PARTITIONED BY (downloaded_date STRING)
STORED AS PARQUET
LOCATION 's3://covid-19-puerto-rico-data/biostatistics.salud.pr.gov/data-sources/parquet_v2/';


--------------------------------------------------------------------------
--------------------------------------------------------------------------
--
-- Deaths
--

CREATE EXTERNAL TABLE biostatistics_sources.deaths_parquet_v1 (
    deathId STRING,
    deathDate STRING,
    deathReportDate STRING,
    sex STRING,
    ageRange STRING,
    physicalRegion STRING,
    vaccinationStatusAtDeath STRING
) PARTITIONED BY (downloaded_date STRING)
STORED AS PARQUET
LOCATION 's3://covid-19-puerto-rico-data/biostatistics.salud.pr.gov/deaths/parquet_v1/';

CREATE EXTERNAL TABLE biostatistics_sources.deaths_parquet_v2 (
    deathId STRING,
    deathDate DATE,
    deathReportDate DATE,
    sex STRING,
    ageRange STRING,
    physicalRegion STRING,
    vaccinationStatusAtDeath STRING,
    downloadedAt TIMESTAMP
) PARTITIONED BY (downloaded_date STRING)
STORED AS PARQUET
LOCATION 's3://covid-19-puerto-rico-data/biostatistics.salud.pr.gov/deaths/parquet_v2/';


--------------------------------------------------------------------------
--------------------------------------------------------------------------
--
-- Cases
--

CREATE EXTERNAL TABLE biostatistics_sources.cases_parquet_v1 (
    caseId STRING,
    caseCategory STRING,
    caseType STRING,
    caseClassification STRING,
    patientId STRING,
    patientAgeRange STRING,
    patientSex STRING,
    patientPhysicalCity STRING,
    patientPhysicalRegion STRING,
    earliestPositiveRankingTestSampleCollectedDate STRING,
    earliestPositiveDiagnosticTestSampleCollectedDate STRING,
    caseCreatedAt STRING
) PARTITIONED BY (downloaded_date STRING)
STORED AS PARQUET
LOCATION 's3://covid-19-puerto-rico-data/biostatistics.salud.pr.gov/cases/parquet_v1/';

CREATE EXTERNAL TABLE biostatistics_sources.cases_parquet_v2 (
    caseId STRING,
    caseCategory STRING,
    caseType STRING,
    caseClassification STRING,
    patientId STRING,
    patientAgeRange STRING,
    patientSex STRING,
    patientPhysicalCity STRING,
    patientPhysicalRegion STRING,
    earliestPositiveRankingTestSampleCollectedDate TIMESTAMP,
    earliestPositiveDiagnosticTestSampleCollectedDate TIMESTAMP,
    caseCreatedAt TIMESTAMP,
    downloadedAt TIMESTAMP
) PARTITIONED BY (downloaded_date STRING)
STORED AS PARQUET
LOCATION 's3://covid-19-puerto-rico-data/biostatistics.salud.pr.gov/cases/parquet_v2/';


--------------------------------------------------------------------------
--------------------------------------------------------------------------
--
-- Test orders
--

CREATE EXTERNAL TABLE biostatistics_sources.tests_parquet_v1 (
    orderTestId STRING,
    patientId STRING,
    patientAgeRange STRING,
    patientRegion STRING,
    patientCity STRING,
    orderTestCategory STRING,
    orderTestType STRING,
    sampleCollectedDate STRING,
    resultReportDate STRING,
    orderTestResult STRING,
    orderTestCreatedAt STRING
) PARTITIONED BY (downloaded_date STRING)
STORED AS PARQUET
LOCATION 's3://covid-19-puerto-rico-data/biostatistics.salud.pr.gov/tests/parquet_v1/';

CREATE EXTERNAL TABLE biostatistics_sources.tests_parquet_v2 (
    orderTestId STRING,
    patientId STRING,
    patientAgeRange STRING,
    patientRegion STRING,
    patientCity STRING,
    orderTestCategory STRING,
    orderTestType STRING,
    sampleCollectedDate TIMESTAMP,
    resultReportDate TIMESTAMP,
    orderTestResult STRING,
    orderTestCreatedAt TIMESTAMP,
    downloadedAt TIMESTAMP
) PARTITIONED BY (downloaded_date STRING)
STORED AS PARQUET
LOCATION 's3://covid-19-puerto-rico-data/biostatistics.salud.pr.gov/tests/parquet_v2/';


CREATE EXTERNAL TABLE biostatistics_sources.tests_grouped_parquet_v1 (
    sampleCollectedDate STRING,
    entity STRING,
    entityCity STRING,
    totalTestsProcessed BIGINT,
    totalMolecularTestsProcessed BIGINT,
    totalMolecularTestsPositive BIGINT,
    totalMolecularTestsNegative BIGINT,
    totalAntigensTestsProcessed BIGINT,
    totalAntigensTestsPositive BIGINT,
    totalAntigensTestsNegative BIGINT
) PARTITIONED BY (downloaded_date STRING)
STORED AS PARQUET
LOCATION 's3://covid-19-puerto-rico-data/biostatistics.salud.pr.gov/tests-grouped/parquet_v1/';

CREATE EXTERNAL TABLE biostatistics_sources.tests_grouped_parquet_v2 (
    sampleCollectedDate DATE,
    entity STRING,
    entityCity STRING,
    totalTestsProcessed BIGINT,
    totalMolecularTestsProcessed BIGINT,
    totalMolecularTestsPositive BIGINT,
    totalMolecularTestsNegative BIGINT,
    totalAntigensTestsProcessed BIGINT,
    totalAntigensTestsPositive BIGINT,
    totalAntigensTestsNegative BIGINT,
    downloadedAt TIMESTAMP
) PARTITIONED BY (downloaded_date STRING)
STORED AS PARQUET
LOCATION 's3://covid-19-puerto-rico-data/biostatistics.salud.pr.gov/tests-grouped/parquet_v2/';


--------------------------------------------------------------------------
--------------------------------------------------------------------------
--
-- Vaccination
--

CREATE EXTERNAL TABLE biostatistics_sources.persons_with_vax_status_parquet_v1 (
    personAgeRange STRING,
    personSex STRING,
    personState STRING,
    personCity STRING,
    personRegion STRING,
    personVaccinationStatus STRING,
    personLastVaccinationDate STRING
) PARTITIONED BY (downloaded_date STRING)
STORED AS PARQUET
LOCATION 's3://covid-19-puerto-rico-data/biostatistics.salud.pr.gov/persons-with-vaccination-status/parquet_v1/';

CREATE EXTERNAL TABLE biostatistics_sources.persons_with_vax_status_parquet_v2 (
    personAgeRange STRING,
    personSex STRING,
    personState STRING,
    personCity STRING,
    personRegion STRING,
    personVaccinationStatus STRING,
    personLastVaccinationDate DATE,
    downloadedAt TIMESTAMP
) PARTITIONED BY (downloaded_date STRING)
STORED AS PARQUET
LOCATION 's3://covid-19-puerto-rico-data/biostatistics.salud.pr.gov/persons-with-vaccination-status/parquet_v2/';
