--
-- Create the schema that reads from the source data files.
--
-- We make zero effort here to clean these files or even
-- to parse the data types and treat them as strings unless
-- it's zero effort to coerce them to right type.
--

CREATE EXTERNAL TABLE covid_pr_sources.bulletin_cases_csv (
    bulletin_date STRING,
    datum_date STRING,
    confirmed_and_probable_cases STRING,
    confirmed_cases STRING,
    probable_cases STRING,
    deaths STRING
) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
LOCATION 's3://covid-19-puerto-rico-data/bulletin/cases/'
TBLPROPERTIES (
    "skip.header.line.count"="1"
);

CREATE EXTERNAL TABLE covid_pr_sources.tests_csv_v1 (
    downloadedAt STRING,
    collectedDate STRING,
    reportedDate STRING,
    ageRange STRING,
    testType STRING,
    result STRING,
    patientCity STRING,
    createdAt STRING
) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
LOCATION 's3://covid-19-puerto-rico-data/bioportal/tests/csv_v1/'
TBLPROPERTIES (
    "skip.header.line.count"="1"
);

CREATE EXTERNAL TABLE covid_pr_sources.tests_csv_v2 (
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
LOCATION 's3://covid-19-puerto-rico-data/bioportal/tests/csv_v2/'
TBLPROPERTIES (
    "skip.header.line.count"="1"
);

CREATE EXTERNAL TABLE covid_pr_sources.cases_csv_v1 (
    downloadedAt STRING,
    patientId STRING,
    collectedDate STRING,
    reportedDate STRING,
    ageRange STRING,
    testType STRING,
    result STRING,
    region STRING,
    createdAt STRING
) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
LOCATION 's3://covid-19-puerto-rico-data/bioportal/cases/csv_v1/'
TBLPROPERTIES (
    "skip.header.line.count"="1"
);