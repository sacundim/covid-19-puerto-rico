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


--------------------------------------------------------------------------
--------------------------------------------------------------------------
--
-- Daily bulletin data
--

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

CREATE EXTERNAL TABLE covid_pr_sources.bulletin_municipal_molecular (
    bulletin_date DATE,
    municipality STRING,
    confirmed_cases INT,
    confirmed_cases_percent DOUBLE
) ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
  ESCAPED BY '\\'
  LINES TERMINATED BY '\n'
LOCATION 's3://covid-19-puerto-rico-data/bulletin/municipal_molecular/'
TBLPROPERTIES (
    "skip.header.line.count"="1"
);

CREATE EXTERNAL TABLE covid_pr_sources.bulletin_municipal_antigens (
    bulletin_date DATE,
    municipality STRING,
    probable_cases INT,
    probable_cases_percent DOUBLE
) ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
  ESCAPED BY '\\'
  LINES TERMINATED BY '\n'
LOCATION 's3://covid-19-puerto-rico-data/bulletin/municipal_antigens/'
TBLPROPERTIES (
    "skip.header.line.count"="1"
);


--------------------------------------------------------------------------
--------------------------------------------------------------------------
--
-- Bioportal data
--

CREATE EXTERNAL TABLE covid_pr_sources.orders_basic_parquet_v2 (
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
) PARTITIONED BY (downloaded_date STRING)
STORED AS PARQUET
LOCATION 's3://covid-19-puerto-rico-data/bioportal/orders-basic/parquet_v2/';

CREATE EXTERNAL TABLE covid_pr_sources.minimal_info_unique_tests_parquet_v4 (
	downloadedAt STRING,
    collectedDate STRING,
    reportedDate STRING,
    ageRange STRING,
    testType STRING,
    result STRING,
    city STRING,
    createdAt STRING
) PARTITIONED BY (downloaded_date STRING)
STORED AS PARQUET
LOCATION 's3://covid-19-puerto-rico-data/bioportal/minimal-info-unique-tests/parquet_v4/';

CREATE EXTERNAL TABLE covid_pr_sources.deaths_parquet_v1 (
	downloadedAt STRING,
    region STRING,
    ageRange STRING,
    sex STRING,
    deathDate STRING,
    reportDate STRING
) PARTITIONED BY (downloaded_date STRING)
STORED AS PARQUET
LOCATION 's3://covid-19-puerto-rico-data/bioportal/deaths/parquet_v1/';
