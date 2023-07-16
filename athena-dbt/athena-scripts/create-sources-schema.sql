--
-- Create the schema that reads from the Bioportal source data files.
--
-- We make zero effort here to clean these files or even
-- to parse the data types and treat them as strings unless
-- it's zero effort to coerce them to right type.
--

DROP DATABASE IF EXISTS bioportal_sources CASCADE;

CREATE DATABASE bioportal_sources
LOCATION 's3://covid-19-puerto-rico-data/bioportal/';


--------------------------------------------------------------------------
--------------------------------------------------------------------------
--
-- Daily bulletin data
--

CREATE EXTERNAL TABLE bioportal_sources.bulletin_cases_csv (
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

CREATE EXTERNAL TABLE bioportal_sources.bulletin_municipal_molecular (
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

CREATE EXTERNAL TABLE bioportal_sources.bulletin_municipal_antigens (
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

CREATE EXTERNAL TABLE bioportal_sources.orders_basic_parquet_v2 (
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

CREATE EXTERNAL TABLE bioportal_sources.orders_basic_parquet_v5 (
    -- No downloadedAt anymore; must get from filename
    ageRange STRING,
    collectedDate STRING,
    orderCreatedAt STRING,
    patientId STRING,
    region STRING,
    reportedDate STRING,
    result STRING,
    resultCreatedAt STRING,
    testType STRING
) PARTITIONED BY (downloaded_date STRING)
STORED AS PARQUET
LOCATION 's3://covid-19-puerto-rico-data/bioportal/orders-basic/parquet_v5/';


CREATE EXTERNAL TABLE bioportal_sources.minimal_info_unique_tests_parquet_v4 (
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

CREATE EXTERNAL TABLE bioportal_sources.minimal_info_unique_tests_parquet_v5 (
    -- No downloadedAt anymore; must get from filename
    ageRange STRING,
    city STRING,
    collectedDate STRING,
    collectedDateUtc STRING,
    createdAt STRING,
    createdAtUtc STRING,
    reportedDate STRING,
    reportedDateUtc STRING,
    result STRING,
    testType STRING
) PARTITIONED BY (downloaded_date STRING)
STORED AS PARQUET
LOCATION 's3://covid-19-puerto-rico-data/bioportal/minimal-info-unique-tests/parquet_v5/';


CREATE EXTERNAL TABLE bioportal_sources.deaths_parquet_v1 (
	downloadedAt STRING,
    region STRING,
    ageRange STRING,
    sex STRING,
    deathDate STRING,
    reportDate STRING
) PARTITIONED BY (downloaded_date STRING)
STORED AS PARQUET
LOCATION 's3://covid-19-puerto-rico-data/bioportal/deaths/parquet_v1/';

CREATE EXTERNAL TABLE bioportal_sources.deaths_parquet_v5 (
    -- No downloadedAt anymore; must get from filename
    ageRange STRING,
    deathDate STRING,
    region STRING,
    sex STRING,
    reportDate STRING
) PARTITIONED BY (downloaded_date STRING)
STORED AS PARQUET
LOCATION 's3://covid-19-puerto-rico-data/bioportal/deaths/parquet_v5/';


--------------------------------------------------------------------------
--------------------------------------------------------------------------
--
-- Walgreens/Aegis data
--

CREATE EXTERNAL TABLE bioportal_sources.walgreens_tracker_parquet_v1 (
    State STRING,
    Date STRING,
    `3day_mov_avgOmiOther` STRING,
    `3day_mov_avgOmiBA2` STRING,
    `3day_mov_avgOmiBA1` STRING,
    `3day_mvOmiBA2_nmrtr` STRING,
    `3day_mvPreOther_nmrtr` STRING,
    `3day_mvPreOmiBA2_nmrtr` STRING,
    `3day_mvPreOmiBA11_nmrtr` STRING,
    `3day_mov_avgPreOmiBA2` STRING,
    `3day_mov_avgPreOmiBA11` STRING,
    `3day_mov_avgPreOther` STRING,
    `3day_mvOmiBA1_nmrtr` STRING,
    `Date_Range` STRING,
    `3day_mvOmiBA4_nmrtr` STRING,
    `3day_mov_avgOmiBA4` STRING,
    `3day_mvOmiBA5_nmrtr` STRING,
    `3day_mov_avgOmiBA5` STRING,
    `3day_mvOmiOther_nmrtr` STRING,
    `3day_mvOmiOtherOmi_nmrtr` STRING,
    `3day_mov_avgOmiOtherOmi` STRING
) STORED AS PARQUET
LOCATION 's3://covid-19-puerto-rico-data/Walgreens/Tracker_Aggregation/parquet_v1/';

CREATE EXTERNAL TABLE bioportal_sources.walgreens_tracker_parquet_v2 (
    State STRING,
    Date BIGINT,
    `3day_mov_avgOmiOther` DOUBLE,
    `3day_mov_avgOmiBA2` DOUBLE,
    `3day_mov_avgOmiBA1` DOUBLE,
    `3day_mvOmiBA2_nmrtr` BIGINT,
    `3day_mvPreOther_nmrtr` BIGINT,
    `3day_mvPreOmiBA2_nmrtr` BIGINT,
    `3day_mvPreOmiBA11_nmrtr` BIGINT,
    `3day_mov_avgPreOmiBA2` DOUBLE,
    `3day_mov_avgPreOmiBA11` DOUBLE,
    `3day_mov_avgPreOther` DOUBLE,
    `3day_mvOmiBA1_nmrtr` BIGINT,
    `Date_Range` STRING,
    `3day_mvOmiBA4_nmrtr` BIGINT,
    `3day_mov_avgOmiBA4` DOUBLE,
    `3day_mvOmiBA5_nmrtr` BIGINT,
    `3day_mov_avgOmiBA5` DOUBLE,
    `3day_mvOmiOther_nmrtr` BIGINT,
    `3day_mvOmiOtherOmi_nmrtr` BIGINT,
    `3day_mov_avgOmiOtherOmi` DOUBLE
) STORED AS PARQUET
LOCATION 's3://covid-19-puerto-rico-data/Walgreens/Tracker_Aggregation/parquet_v2/';

CREATE EXTERNAL TABLE bioportal_sources.walgreens_tracker_parquet_v3 (
    State STRING,
    Date TIMESTAMP,
    `3day_mov_avgOmiOther` DOUBLE,
    `3day_mov_avgOmiBA2` DOUBLE,
    `3day_mov_avgOmiBA1` DOUBLE,
    `3day_mvOmiBA2_nmrtr` BIGINT,
    `3day_mvPreOther_nmrtr` BIGINT,
    `3day_mvPreOmiBA2_nmrtr` BIGINT,
    `3day_mvPreOmiBA11_nmrtr` BIGINT,
    `3day_mov_avgPreOmiBA2` DOUBLE,
    `3day_mov_avgPreOmiBA11` DOUBLE,
    `3day_mov_avgPreOther` DOUBLE,
    `3day_mvOmiBA1_nmrtr` BIGINT,
    `Date_Range` STRING,
    `3day_mvOmiBA4_nmrtr` BIGINT,
    `3day_mov_avgOmiBA4` DOUBLE,
    `3day_mvOmiBA5_nmrtr` BIGINT,
    `3day_mov_avgOmiBA5` DOUBLE,
    `3day_mvOmiOther_nmrtr` BIGINT,
    `3day_mvOmiOtherOmi_nmrtr` BIGINT,
    `3day_mov_avgOmiOtherOmi` DOUBLE,
    downloaded_at TIMESTAMP
) STORED AS PARQUET
LOCATION 's3://covid-19-puerto-rico-data/Walgreens/Tracker_Aggregation/parquet_v3/';
