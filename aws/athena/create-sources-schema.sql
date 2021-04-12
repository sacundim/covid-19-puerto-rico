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

CREATE VIEW covid_pr_sources.bulletin_municipal AS
SELECT
	bulletin_date,
	municipality,
	COALESCE(confirmed_cases, 0) + COALESCE(probable_cases, 0)
		AS cumulative_cases,
	COALESCE(confirmed_cases, 0)
		+ COALESCE(probable_cases, 0)
		- COALESCE(lag(confirmed_cases, 1, 0) OVER (
			PARTITION BY municipality
			ORDER BY bulletin_date
		), 0)
		- COALESCE(lag(probable_cases, 1, 0) OVER (
			PARTITION BY municipality
			ORDER BY bulletin_date
		), 0) AS delta_cases
FROM covid_pr_sources.bulletin_municipal_molecular pcr
FULL OUTER JOIN covid_pr_sources.bulletin_municipal_antigens anti
	USING (bulletin_date, municipality)
ORDER BY bulletin_date, municipality;


--------------------------------------------------------------------------
--------------------------------------------------------------------------
--
-- Bioportal data
--

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
	age_gte STRING,
	age_lt STRING
)  ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
LOCATION 's3://covid-19-puerto-rico-data/Census/acs_2019_1y_age_ranges/'
TBLPROPERTIES (
    "skip.header.line.count"="1"
);

CREATE VIEW covid_pr_sources.acs_2019_1y_age_ranges AS
SELECT
	age_range,
	CAST(population AS INTEGER) AS population,
	CAST(age_gte AS INTEGER) AS age_gte,
	CAST(nullif(age_lt, '') AS INTEGER) AS age_lt
FROM covid_pr_sources.acs_2019_1y_age_ranges_csv;


CREATE EXTERNAL TABLE covid_pr_sources.bioportal_age_ranges_csv (
	age_range STRING,
	age_gte STRING,
	age_lt STRING
)  ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
LOCATION 's3://covid-19-puerto-rico-data/Census/bioportal_age_ranges/'
TBLPROPERTIES (
    "skip.header.line.count"="1"
);

CREATE VIEW covid_pr_sources.bioportal_age_ranges AS
SELECT
	age_range,
	CAST(age_gte AS INTEGER) AS age_gte,
	CAST(nullif(age_lt, '') AS INTEGER) AS age_lt
FROM covid_pr_sources.bioportal_age_ranges_csv;


CREATE EXTERNAL TABLE covid_pr_sources.age_range_reln_csv (
	bioportal_age_gte STRING,
	acs_age_gte STRING,
	prdoh_age_gte STRING,
	four_band_age_gte STRING
)  ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
LOCATION 's3://covid-19-puerto-rico-data/Census/age_range_reln/'
TBLPROPERTIES (
    "skip.header.line.count"="1"
);

CREATE VIEW covid_pr_sources.age_range_reln AS
SELECT
	CAST(bioportal_age_gte AS INTEGER) AS bioportal_age_gte,
	CAST(acs_age_gte AS INTEGER) AS acs_age_gte,
	CAST(prdoh_age_gte AS INTEGER) AS prdoh_age_gte,
	CAST(four_band_age_gte AS INTEGER) AS four_band_age_gte
FROM covid_pr_sources.age_range_reln_csv;


CREATE EXTERNAL TABLE covid_pr_sources.prdoh_age_ranges_csv (
	age_range STRING,
	age_gte STRING,
	age_lt STRING
)  ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
LOCATION 's3://covid-19-puerto-rico-data/Census/prdoh_age_ranges/'
TBLPROPERTIES (
    "skip.header.line.count"="1"
);

CREATE VIEW covid_pr_sources.prdoh_age_ranges AS
SELECT
    age_range,
	CAST(age_gte AS INTEGER) AS age_gte,
	CAST(nullif(age_lt, '') AS INTEGER) AS age_lt
FROM covid_pr_sources.prdoh_age_ranges_csv;


-----------------------------------------------------------------------------
-----------------------------------------------------------------------------
--
-- Tables and views For Census Bureau ACS municipal data
--

CREATE EXTERNAL TABLE covid_pr_sources.acs_2019_5y_municipal_race (
    municipality STRING,
    fips STRING,
    population INT,
    white_alone INT,
    white_alone_margin INT,
    black_alone INT,
    black_alone_margin INT,
    other_alone INT,
    other_margin INT,
    two_or_more INT,
    two_or_more_margin INT
) ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
  ESCAPED BY '\\'
  LINES TERMINATED BY '\n'
LOCATION 's3://covid-19-puerto-rico-data/Census/acs_2019_5y_municipal_race/'
TBLPROPERTIES (
    "skip.header.line.count"="1"
);

CREATE EXTERNAL TABLE covid_pr_sources.acs_2019_5y_municipal_household_income (
    municipality STRING,
    fips STRING,
    households INT,
    households_error INT,
    households_lt_10k_pct DOUBLE,
    households_lt_10k_error_pct DOUBLE,
    households_10k_15k_pct DOUBLE,
    households_10k_15k_error_pct DOUBLE,
    households_15k_25k_pct DOUBLE,
    households_15k_25k_error_pct DOUBLE,
    households_25k_35k_pct DOUBLE,
    households_25k_35k_error_pct DOUBLE,
    households_35k_50k_pct DOUBLE,
    households_35k_50k_error_pct DOUBLE,
    households_50k_75k_pct DOUBLE,
    households_50k_75k_error_pct DOUBLE,
    households_75k_100k_pct DOUBLE,
    households_75k_100k_error_pct DOUBLE,
    households_100k_150k_pct DOUBLE,
    households_100k_150k_error_pct DOUBLE,
    households_150k_200k_pct DOUBLE,
    households_150k_200k_error_pct DOUBLE,
    households_gte_200k_pct DOUBLE,
    households_gte_200k_error_pct DOUBLE,
    households_median INT,
    households_median_error INT,
    households_mean INT,
    households_mean_error INT
) ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
  ESCAPED BY '\\'
  LINES TERMINATED BY '\n'
LOCATION 's3://covid-19-puerto-rico-data/Census/acs_2019_5y_municipal_household_income/'
TBLPROPERTIES (
    "skip.header.line.count"="1"
);

