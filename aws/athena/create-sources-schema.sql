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


-----------------------------------------------------------------------------
-----------------------------------------------------------------------------
--
-- Tables and views to join Bioportal age_range data to Census Bureau population
--

CREATE EXTERNAL TABLE covid_pr_sources.acs_2019_1y_age_ranges_csv (
	age_range STRING,
	population STRING,
	youngest STRING,
	next STRING
)  ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
LOCATION 's3://covid-19-puerto-rico-data/Census/acs_2019_1y_age_ranges/'
TBLPROPERTIES (
    "skip.header.line.count"="1"
);

CREATE VIEW covid_pr_sources.acs_2019_1y_age_ranges AS
SELECT
	age_range,
	CAST(population AS INTEGER) AS population,
	CAST(youngest AS INTEGER) AS youngest,
	CAST(nullif(next, '') AS INTEGER) AS next
FROM covid_pr_sources.acs_2019_1y_age_ranges_csv;


CREATE EXTERNAL TABLE covid_pr_sources.bioportal_age_ranges_csv (
	age_range STRING,
	youngest STRING,
	next STRING
)  ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
LOCATION 's3://covid-19-puerto-rico-data/Census/bioportal_age_ranges/'
TBLPROPERTIES (
    "skip.header.line.count"="1"
);

CREATE VIEW covid_pr_sources.bioportal_age_ranges AS
SELECT
	age_range,
	CAST(youngest AS INTEGER) AS youngest,
	CAST(nullif(next, '') AS INTEGER) AS next
FROM covid_pr_sources.bioportal_age_ranges_csv;


CREATE EXTERNAL TABLE covid_pr_sources.age_range_reln_csv (
	bioportal_youngest STRING,
	acs_youngest STRING,
	prdoh_youngest STRING,
	four_band_youngest STRING
)  ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
LOCATION 's3://covid-19-puerto-rico-data/Census/age_range_reln/'
TBLPROPERTIES (
    "skip.header.line.count"="1"
);

CREATE VIEW covid_pr_sources.age_range_reln AS
SELECT
	CAST(bioportal_youngest AS INTEGER) AS bioportal_youngest,
	CAST(acs_youngest AS INTEGER) AS acs_youngest,
	CAST(prdoh_youngest AS INTEGER) AS prdoh_youngest,
	CAST(four_band_youngest AS INTEGER) AS four_band_youngest
FROM covid_pr_sources.age_range_reln_csv;


CREATE EXTERNAL TABLE covid_pr_sources.prdoh_age_ranges_csv (
	age_range STRING,
	youngest STRING,
	next STRING
)  ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
LOCATION 's3://covid-19-puerto-rico-data/Census/prdoh_age_ranges/'
TBLPROPERTIES (
    "skip.header.line.count"="1"
);

CREATE VIEW covid_pr_sources.prdoh_age_ranges AS
SELECT
    age_range,
	CAST(youngest AS INTEGER) AS youngest,
	CAST(nullif(next, '') AS INTEGER) AS next
FROM covid_pr_sources.prdoh_age_ranges_csv;
