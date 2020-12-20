--
-- Create the schema that reads from the source data files.
--
-- We make zero effort here to clean these files or even
-- to parse the data types and treat them as strings unless
-- it's zero effort to coerce them to right type.
--

DROP DATABASE IF EXISTS covid_pr_sources CASCADE;

CREATE DATABASE covid_pr_sources
LOCATION 's3://covid-19-puerto-rico-data/';

CREATE EXTERNAL TABLE covid_pr_sources.bulletin_cases_csv (
    bulletin_date STRING,
    datum_date STRING,
    confirmed_and_suspect_cases STRING,
    confirmed_cases STRING,
    probable_cases STRING,
    suspect_cases STRING,
    deaths STRING
) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
LOCATION 's3://covid-19-puerto-rico-data/bulletin/cases/'
TBLPROPERTIES (
    "skip.header.line.count"="1"
);

CREATE EXTERNAL TABLE covid_pr_sources.orders_basic_parquet_v1 (
    downloadedAt STRING,
    patientId STRING,
    collectedDate STRING,
    reportedDate STRING,
    ageRange STRING,
    testType STRING,
    result STRING,
    region STRING,
    orderCreatedAt STRING,
    resultCreatedAt STRING
) STORED AS PARQUET
LOCATION 's3://covid-19-puerto-rico-data/bioportal/orders-basic/parquet_v1/';

CREATE EXTERNAL TABLE covid_pr_sources.minimal_info_unique_tests_parquet_v3 (
    downloadedAt STRING,
    collectedDate STRING,
    reportedDate STRING,
    ageRange STRING,
    testType STRING,
    result STRING,
    city STRING,
    createdAt STRING
) STORED AS PARQUET
LOCATION 's3://covid-19-puerto-rico-data/bioportal/minimal-info-unique-tests/parquet_v3/';

CREATE EXTERNAL TABLE covid_pr_sources.minimal_info_parquet_v1 (
    downloadedAt STRING,
    patientId STRING,
    collectedDate STRING,
    reportedDate STRING,
    ageRange STRING,
    testType STRING,
    result STRING,
    region STRING,
    createdAt STRING
) STORED AS PARQUET
LOCATION 's3://covid-19-puerto-rico-data/bioportal/minimal-info/parquet_v1/';