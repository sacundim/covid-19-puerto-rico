--
-- A DuckDB schema for the various data collected in here.  The paths
-- are relative to this SQL file's directory; to load into DuckDB CLI,
-- `cd` into here, run the `duckdb` command, and issue the command
-- `.read duckdb.sql`.
--

-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
--
-- Data I copied mostly from the Puerto Rico Department of Health's COVID-19
-- PDF bulletin series, which ran from 2020-04-25 to 2021-07-21.  These PDF
-- files are archived in this repo under `../source_material/pdf/`.
--
-- I've filled in some earlier dates with older data that I compiled by hand
-- from other sources.
--

--
-- This table is the tour-de-force of this CSV collection. These are all of
-- the cases and deaths by event date tables from all of the Puerto Rico
-- Department of Health PDF reports from 2020-04-25 to 2021-07-21, hand
-- extracted and verified, in one single CSV file.
--
-- The original PDF reports' tables record cases by the date that the
-- test sample was taken, and deaths by the date of actual death.  This
-- CSV file has those dates as the `datum_date` column.
--
-- Each PDF report's cover had a "data up to" date in the front page.  This
-- CSV file has those dates as the `bulletin_date` column.
--
CREATE TABLE puerto_rico_bitemporal AS
SELECT *
FROM read_csv_auto(
  'cases/PuertoRico-bitemporal.csv',
  types={'probable_cases': 'BIGINT'}
);


--
-- Case numbers mostly from the front page summaries of the PDF bulletins.
-- By "mostly" I mean that figures before more or less 2020-04-25 are actually
-- what I pieced together from other sources.
--
CREATE TABLE puerto_rico_bulletin AS
SELECT *
FROM read_csv_auto('cases/PuertoRico-bulletin.csv');

--
-- Daily "probable" (= antigen) case by age tables from the PDF bulletins.
--
CREATE TABLE age_groups_antigens AS
SELECT *
FROM read_csv_auto('cases/AgeGroups-antigens.csv');

--
-- Daily "confirmed" (= molecular) case by age tables from the PDF bulletins.
--
CREATE TABLE age_groups_molecular AS
SELECT *
FROM read_csv_auto('cases/AgeGroups-molecular.csv');

--
-- Auxiliary table that I put together with Census Bureau Puerto Rico
-- 2019 population estimates by age group. Crafted to join with the
-- `age_groups_antigens` and `age_groups_molecular` tables.
--
CREATE TABLE age_groups_population AS
SELECT *
FROM read_csv_auto('cases/AgeGroups-population.csv');

--
-- Daily "confirmed" (= antigen) case by municipality from the PDF bulletins.
--
CREATE TABLE municipalities_antigens AS
SELECT *
FROM read_csv_auto('cases/Municipalities-antigens.csv');

--
-- Daily "confirmed" (= antigen) case by municipality from the PDF bulletins.
--
CREATE TABLE municipalities_molecular AS
SELECT *
FROM read_csv_auto('cases/Municipalities-molecular.csv');

--
-- Auxiliary table that I put together with Census Bureau Puerto Rico
-- 2019 population estimates by municipality. Crafted to join with the
-- `municipalities_antigens` and `municipalities_molecular` tables.
--
CREATE TABLE municipalities_canonical_names AS
SELECT *
FROM read_csv_auto(
  'cases/Municipalities-canonical_names.csv',
  types={'fips_code': 'VARCHAR'}
);



-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
--
-- Other miscellaneous data
--

--
-- Some extremely early (up to 2020-09-12) hospitalizations data that
-- was irregularly shared with journalists by the Puerto Rico Department
-- of Health, and in one case even irregularly tweeted out by the Secretary.
--
CREATE TABLE puerto_rico_hospitalizations AS
SELECT *
FROM read_csv_auto('cases/PuertoRico-hospitalizations.csv');


--
-- Very early (up to 2020-07-05) PCR testing volume data that the
-- Puerto Rico Public Health Trust collected from labs. These were
-- shared with me as Excel spreadsheets that I hand-extracted the
-- CSV from.
--
CREATE TABLE PRPHT_molecular AS
SELECT *
FROM read_csv_auto('cases/PRPHT-molecular.csv');


--
-- From 2020-05-21 to 2020-07-23, the Puerto Rico Department of Health
-- irregularly put out seven "Bioportal" reports that detailed molecular
-- testing volume.  This table is the "headline" figures from them.
--
CREATE TABLE puerto_rico_bioportal AS
SELECT *
FROM read_csv_auto('cases/PuertoRico-bioportal.csv');

--
-- The one time on 2020-05-20 they also put out a table of test volumes
-- and results by collected date.
--
CREATE TABLE puerto_rico_bioportal_bitemporal AS
SELECT *
FROM read_csv_auto('cases/PuertoRico-bioportal-bitemporal.csv');


--
-- The Puerto Rico rows of the final (2021-03-07) version of the
-- Covid Tracking Project's dataset.
--
-- https://covidtracking.com/
--
CREATE TABLE covidtracking_puerto_rico_history AS 
SELECT *
FROM read_csv_auto(
  'CovidTracking/puerto-rico-history.csv',
  types={
    'hospitalized': 'BIGINT',
    'hospitalizedCumulative': 'BIGINT',
    'inIcuCumulative': 'BIGINT',
    'negativeTestsAntibody': 'BIGINT',
    'negativeTestsPeopleAntibody': 'BIGINT',
    'onVentilatorCumulative': 'BIGINT',
    'positiveTestsAntigen': 'BIGINT',
    'positiveTestsPeopleAntibody': 'BIGINT',
    'positiveTestsPeopleAntigen': 'BIGINT',
    'totalTestEncountersViral': 'BIGINT',
    'totalTestsAntibody': 'BIGINT',
    'totalTestsAntigen': 'BIGINT',
    'totalTestsPeopleAntibody': 'BIGINT',
    'totalTestsPeopleAntigen': 'BIGINT',
    'totalTestsPeopleViral': 'BIGINT'
  }
);
