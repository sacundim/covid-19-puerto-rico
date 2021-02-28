--
-- Create the schema that reads from the HHS source data files.
--
-- We make zero effort here to clean these files or even
-- to parse the data types and treat them as strings unless
-- it's zero effort to coerce them to the right type.
--

DROP DATABASE IF EXISTS covid_hhs_sources CASCADE;

CREATE DATABASE covid_hhs_sources
LOCATION 's3://covid-19-puerto-rico-data/HHS/';


--
-- https://healthdata.gov/dataset/covid-19-reported-patient-impact-and-hospital-capacity-state-timeseries
--
CREATE EXTERNAL TABLE covid_hhs_sources.reported_hospital_utilization_timeseries (
    state STRING,
    date STRING,
    critical_staffing_shortage_today_yes STRING,
    critical_staffing_shortage_today_no STRING,
    critical_staffing_shortage_today_not_reported STRING,
    critical_staffing_shortage_anticipated_within_week_yes STRING,
    critical_staffing_shortage_anticipated_within_week_no STRING,
    critical_staffing_shortage_anticipated_within_week_not_reported STRING,
    hospital_onset_covid STRING,
    hospital_onset_covid_coverage STRING,
    inpatient_beds STRING,
    inpatient_beds_coverage STRING,
    inpatient_beds_used STRING,
    inpatient_beds_used_coverage STRING,
    inpatient_beds_used_covid STRING,
    inpatient_beds_used_covid_coverage STRING,
    previous_day_admission_adult_covid_confirmed STRING,
    previous_day_admission_adult_covid_confirmed_coverage STRING,
    previous_day_admission_adult_covid_suspected STRING,
    previous_day_admission_adult_covid_suspected_coverage STRING,
    previous_day_admission_pediatric_covid_confirmed STRING,
    previous_day_admission_pediatric_covid_confirmed_coverage STRING,
    previous_day_admission_pediatric_covid_suspected STRING,
    previous_day_admission_pediatric_covid_suspected_coverage STRING,
    staffed_adult_icu_bed_occupancy STRING,
    staffed_adult_icu_bed_occupancy_coverage STRING,
    staffed_icu_adult_patients_confirmed_and_suspected_covid STRING,
    staffed_icu_adult_patients_confirmed_and_suspected_covid_coverage STRING,
    staffed_icu_adult_patients_confirmed_covid STRING,
    staffed_icu_adult_patients_confirmed_covid_coverage STRING,
    total_adult_patients_hospitalized_confirmed_and_suspected_covid STRING,
    total_adult_patients_hospitalized_confirmed_and_suspected_covid_coverage STRING,
    total_adult_patients_hospitalized_confirmed_covid STRING,
    total_adult_patients_hospitalized_confirmed_covid_coverage STRING,
    total_pediatric_patients_hospitalized_confirmed_and_suspected_covid STRING,
    total_pediatric_patients_hospitalized_confirmed_and_suspected_covid_coverage STRING,
    total_pediatric_patients_hospitalized_confirmed_covid STRING,
    total_pediatric_patients_hospitalized_confirmed_covid_coverage STRING,
    total_staffed_adult_icu_beds STRING,
    total_staffed_adult_icu_beds_coverage STRING,
    inpatient_beds_utilization STRING,
    inpatient_beds_utilization_coverage STRING,
    inpatient_beds_utilization_numerator STRING,
    inpatient_beds_utilization_denominator STRING,
    percent_of_inpatients_with_covid STRING,
    percent_of_inpatients_with_covid_coverage STRING,
    percent_of_inpatients_with_covid_numerator STRING,
    percent_of_inpatients_with_covid_denominator STRING,
    inpatient_bed_covid_utilization STRING,
    inpatient_bed_covid_utilization_coverage STRING,
    inpatient_bed_covid_utilization_numerator STRING,
    inpatient_bed_covid_utilization_denominator STRING,
    adult_icu_bed_covid_utilization STRING,
    adult_icu_bed_covid_utilization_coverage STRING,
    adult_icu_bed_covid_utilization_numerator STRING,
    adult_icu_bed_covid_utilization_denominator STRING,
    adult_icu_bed_utilization STRING,
    adult_icu_bed_utilization_coverage STRING,
    adult_icu_bed_utilization_numerator STRING,
    adult_icu_bed_utilization_denominator STRING
) STORED AS PARQUET
LOCATION 's3://covid-19-puerto-rico-data/HHS/reported_hospital_utilization_timeseries/parquet/';


--
-- https://healthdata.gov/dataset/covid-19-reported-patient-impact-and-hospital-capacity-state
--
CREATE EXTERNAL TABLE covid_hhs_sources.reported_hospital_utilization (
    state CHAR(2),
    critical_staffing_shortage_today_yes STRING,
    critical_staffing_shortage_today_no STRING,
    critical_staffing_shortage_today_not_reported STRING,
    critical_staffing_shortage_anticipated_within_week_yes STRING,
    critical_staffing_shortage_anticipated_within_week_no STRING,
    critical_staffing_shortage_anticipated_within_week_not_reported STRING,
    hospital_onset_covid STRING,
    hospital_onset_covid_coverage STRING,
    inpatient_beds STRING,
    inpatient_beds_coverage STRING,
    inpatient_beds_used STRING,
    inpatient_beds_used_coverage STRING,
    inpatient_beds_used_covid STRING,
    inpatient_beds_used_covid_coverage STRING,
    previous_day_admission_adult_covid_confirmed STRING,
    previous_day_admission_adult_covid_confirmed_coverage STRING,
    previous_day_admission_adult_covid_suspected STRING,
    previous_day_admission_adult_covid_suspected_coverage STRING,
    previous_day_admission_pediatric_covid_confirmed STRING,
    previous_day_admission_pediatric_covid_confirmed_coverage STRING,
    previous_day_admission_pediatric_covid_suspected STRING,
    previous_day_admission_pediatric_covid_suspected_coverage STRING,
    staffed_adult_icu_bed_occupancy STRING,
    staffed_adult_icu_bed_occupancy_coverage STRING,
    staffed_icu_adult_patients_confirmed_and_suspected_covid STRING,
    staffed_icu_adult_patients_confirmed_and_suspected_covid_coverage STRING,
    staffed_icu_adult_patients_confirmed_covid STRING,
    staffed_icu_adult_patients_confirmed_covid_coverage STRING,
    total_adult_patients_hospitalized_confirmed_and_suspected_covid STRING,
    total_adult_patients_hospitalized_confirmed_and_suspected_covid_coverage STRING,
    total_adult_patients_hospitalized_confirmed_covid STRING,
    total_adult_patients_hospitalized_confirmed_covid_coverage STRING,
    total_pediatric_patients_hospitalized_confirmed_and_suspected_covid STRING,
    total_pediatric_patients_hospitalized_confirmed_and_suspected_covid_coverage STRING,
    total_pediatric_patients_hospitalized_confirmed_covid STRING,
    total_pediatric_patients_hospitalized_confirmed_covid_coverage STRING,
    total_staffed_adult_icu_beds STRING,
    total_staffed_adult_icu_beds_coverage STRING,
    inpatient_beds_utilization STRING,
    inpatient_beds_utilization_coverage STRING,
    inpatient_beds_utilization_numerator STRING,
    inpatient_beds_utilization_denominator STRING,
    percent_of_inpatients_with_covid STRING,
    percent_of_inpatients_with_covid_coverage STRING,
    percent_of_inpatients_with_covid_numerator STRING,
    percent_of_inpatients_with_covid_denominator STRING,
    inpatient_bed_covid_utilization STRING,
    inpatient_bed_covid_utilization_coverage STRING,
    inpatient_bed_covid_utilization_numerator STRING,
    inpatient_bed_covid_utilization_denominator STRING,
    adult_icu_bed_covid_utilization STRING,
    adult_icu_bed_covid_utilization_coverage STRING,
    adult_icu_bed_covid_utilization_numerator STRING,
    adult_icu_bed_covid_utilization_denominator STRING,
    adult_icu_bed_utilization STRING,
    adult_icu_bed_utilization_coverage STRING,
    adult_icu_bed_utilization_numerator STRING,
    adult_icu_bed_utilization_denominator STRING,
    reporting_cutoff_start STRING
) STORED AS PARQUET
LOCATION 's3://covid-19-puerto-rico-data/HHS/reported_hospital_utilization/parquet/';

--
-- https://healthdata.gov/dataset/covid-19-reported-patient-impact-and-hospital-capacity-facility
--
CREATE EXTERNAL TABLE covid_hhs_sources.reported_hospital_capacity_admissions_facility_level_weekly_average_timeseries (
    `hospital_pk` STRING,
    `collection_week` STRING,
    `state` CHAR(2),
    `ccn` CHAR(6),
    `hospital_name` STRING,
    `address` STRING,
    `city` STRING,
    `zip` CHAR(5),
    `hospital_subtype` STRING,
    `fips_code` CHAR(5),
    `is_metro_micro` STRING,
    `total_beds_7_day_avg` STRING,
    `all_adult_hospital_beds_7_day_avg` STRING,
    `all_adult_hospital_inpatient_beds_7_day_avg` STRING,
    `inpatient_beds_used_7_day_avg` STRING,
    `all_adult_hospital_inpatient_bed_occupied_7_day_avg` STRING,
    `total_adult_patients_hospitalized_confirmed_and_suspected_covid_7_day_avg` STRING,
    `total_adult_patients_hospitalized_confirmed_covid_7_day_avg` STRING,
    `total_pediatric_patients_hospitalized_confirmed_and_suspected_covid_7_day_avg` STRING,
    `total_pediatric_patients_hospitalized_confirmed_covid_7_day_avg` STRING,
    `inpatient_beds_7_day_avg` STRING,
    `total_icu_beds_7_day_avg` STRING,
    `total_staffed_adult_icu_beds_7_day_avg` STRING,
    `icu_beds_used_7_day_avg` STRING,
    `staffed_adult_icu_bed_occupancy_7_day_avg` STRING,
    `staffed_icu_adult_patients_confirmed_and_suspected_covid_7_day_avg` STRING,
    `staffed_icu_adult_patients_confirmed_covid_7_day_avg` STRING,
    `total_patients_hospitalized_confirmed_influenza_7_day_avg` STRING,
    `icu_patients_confirmed_influenza_7_day_avg` STRING,
    `total_patients_hospitalized_confirmed_influenza_and_covid_7_day_avg` STRING,
    `total_beds_7_day_sum` STRING,
    `all_adult_hospital_beds_7_day_sum` STRING,
    `all_adult_hospital_inpatient_beds_7_day_sum` STRING,
    `inpatient_beds_used_7_day_sum` STRING,
    `all_adult_hospital_inpatient_bed_occupied_7_day_sum` STRING,
    `total_adult_patients_hospitalized_confirmed_and_suspected_covid_7_day_sum` STRING,
    `total_adult_patients_hospitalized_confirmed_covid_7_day_sum` STRING,
    `total_pediatric_patients_hospitalized_confirmed_and_suspected_covid_7_day_sum` STRING,
    `total_pediatric_patients_hospitalized_confirmed_covid_7_day_sum` STRING,
    `inpatient_beds_7_day_sum` STRING,
    `total_icu_beds_7_day_sum` STRING,
    `total_staffed_adult_icu_beds_7_day_sum` STRING,
    `icu_beds_used_7_day_sum` STRING,
    `staffed_adult_icu_bed_occupancy_7_day_sum` STRING,
    `staffed_icu_adult_patients_confirmed_and_suspected_covid_7_day_sum` STRING,
    `staffed_icu_adult_patients_confirmed_covid_7_day_sum` STRING,
    `total_patients_hospitalized_confirmed_influenza_7_day_sum` STRING,
    `icu_patients_confirmed_influenza_7_day_sum` STRING,
    `total_patients_hospitalized_confirmed_influenza_and_covid_7_day_sum` STRING,
    `total_beds_7_day_coverage` STRING,
    `all_adult_hospital_beds_7_day_coverage` STRING,
    `all_adult_hospital_inpatient_beds_7_day_coverage` STRING,
    `inpatient_beds_used_7_day_coverage` STRING,
    `all_adult_hospital_inpatient_bed_occupied_7_day_coverage` STRING,
    `total_adult_patients_hospitalized_confirmed_and_suspected_covid_7_day_coverage` STRING,
    `total_adult_patients_hospitalized_confirmed_covid_7_day_coverage` STRING,
    `total_pediatric_patients_hospitalized_confirmed_and_suspected_covid_7_day_coverage` STRING,
    `total_pediatric_patients_hospitalized_confirmed_covid_7_day_coverage` STRING,
    `inpatient_beds_7_day_coverage` STRING,
    `total_icu_beds_7_day_coverage` STRING,
    `total_staffed_adult_icu_beds_7_day_coverage` STRING,
    `icu_beds_used_7_day_coverage` STRING,
    `staffed_adult_icu_bed_occupancy_7_day_coverage` STRING,
    `staffed_icu_adult_patients_confirmed_and_suspected_covid_7_day_coverage` STRING,
    `staffed_icu_adult_patients_confirmed_covid_7_day_coverage` STRING,
    `total_patients_hospitalized_confirmed_influenza_7_day_coverage` STRING,
    `icu_patients_confirmed_influenza_7_day_coverage` STRING,
    `total_patients_hospitalized_confirmed_influenza_and_covid_7_day_coverage` STRING,
    `previous_day_admission_adult_covid_confirmed_7_day_sum` STRING,
    `previous_day_admission_adult_covid_confirmed_18-19_7_day_sum` STRING,
    `previous_day_admission_adult_covid_confirmed_20-29_7_day_sum` STRING,
    `previous_day_admission_adult_covid_confirmed_30-39_7_day_sum` STRING,
    `previous_day_admission_adult_covid_confirmed_40-49_7_day_sum` STRING,
    `previous_day_admission_adult_covid_confirmed_50-59_7_day_sum` STRING,
    `previous_day_admission_adult_covid_confirmed_60-69_7_day_sum` STRING,
    `previous_day_admission_adult_covid_confirmed_70-79_7_day_sum` STRING,
    `previous_day_admission_adult_covid_confirmed_80+_7_day_sum` STRING,
    `previous_day_admission_adult_covid_confirmed_unknown_7_day_sum` STRING,
    `previous_day_admission_pediatric_covid_confirmed_7_day_sum` STRING,
    `previous_day_covid_ED_visits_7_day_sum` STRING,
    `previous_day_admission_adult_covid_suspected_7_day_sum` STRING,
    `previous_day_admission_adult_covid_suspected_18-19_7_day_sum` STRING,
    `previous_day_admission_adult_covid_suspected_20-29_7_day_sum` STRING,
    `previous_day_admission_adult_covid_suspected_30-39_7_day_sum` STRING,
    `previous_day_admission_adult_covid_suspected_40-49_7_day_sum` STRING,
    `previous_day_admission_adult_covid_suspected_50-59_7_day_sum` STRING,
    `previous_day_admission_adult_covid_suspected_60-69_7_day_sum` STRING,
    `previous_day_admission_adult_covid_suspected_70-79_7_day_sum` STRING,
    `previous_day_admission_adult_covid_suspected_80+_7_day_sum` STRING,
    `previous_day_admission_adult_covid_suspected_unknown_7_day_sum` STRING,
    `previous_day_admission_pediatric_covid_suspected_7_day_sum` STRING,
    `previous_day_total_ED_visits_7_day_sum` STRING,
    `previous_day_admission_influenza_confirmed_7_day_sum` STRING
) STORED AS PARQUET
LOCATION 's3://covid-19-puerto-rico-data/HHS/reported_hospital_capacity_admissions_facility_level_weekly_average_timeseries/parquet/'
;


-------------------------------------------------------------------------
-------------------------------------------------------------------------
--
-- Minimal cleanup and type handling, plus filtering to Puerto Rico
--

CREATE OR REPLACE VIEW covid_hhs_sources.reported_hospital_utilization_timeseries_PR AS
SELECT
	date_parse(regexp_extract("$path", '202[012](\d{4})_(\d{4})'), '%Y%m%d_%H%i')
		AS file_timestamp,
	date(date) AS date,
	CAST(NULLIF(inpatient_beds_used, '') AS INTEGER)
		AS inpatient_beds_used,
	CAST(NULLIF(inpatient_beds_used_coverage , '') AS INTEGER)
		AS inpatient_beds_used_coverage,
	CAST(NULLIF(inpatient_beds_used_covid, '') AS INTEGER)
		AS inpatient_beds_used_covid,
	CAST(NULLIF(inpatient_beds_used_covid_coverage, '') AS INTEGER)
		AS inpatient_beds_used_covid_coverage,
	CAST(NULLIF(previous_day_admission_adult_covid_confirmed, '') AS INTEGER)
		AS previous_day_admission_adult_covid_confirmed,
	CAST(NULLIF(previous_day_admission_adult_covid_confirmed_coverage, '') AS INTEGER)
		AS previous_day_admission_adult_covid_confirmed_coverage,
	CAST(NULLIF(previous_day_admission_adult_covid_suspected, '') AS INTEGER)
		AS previous_day_admission_adult_covid_suspected,
	CAST(NULLIF(previous_day_admission_adult_covid_suspected_coverage, '') AS INTEGER)
		AS previous_day_admission_adult_covid_suspected_coverage,
	CAST(NULLIF(previous_day_admission_pediatric_covid_confirmed, '') AS INTEGER)
		AS previous_day_admission_pediatric_covid_confirmed,
	CAST(NULLIF(previous_day_admission_pediatric_covid_confirmed_coverage, '') AS INTEGER)
		AS previous_day_admission_pediatric_covid_confirmed_coverage,
	CAST(NULLIF(previous_day_admission_pediatric_covid_suspected, '') AS INTEGER)
		AS previous_day_admission_pediatric_covid_suspected,
	CAST(NULLIF(previous_day_admission_pediatric_covid_suspected_coverage, '') AS INTEGER)
		AS previous_day_admission_pediatric_covid_suspected_coverage,
	CAST(NULLIF(staffed_adult_icu_bed_occupancy, '') AS INTEGER)
		AS staffed_adult_icu_bed_occupancy,
	CAST(NULLIF(staffed_adult_icu_bed_occupancy_coverage, '') AS INTEGER)
		AS staffed_adult_icu_bed_occupancy_coverage,
	CAST(NULLIF(staffed_icu_adult_patients_confirmed_and_suspected_covid, '') AS INTEGER)
		AS staffed_icu_adult_patients_confirmed_and_suspected_covid,
	CAST(NULLIF(staffed_icu_adult_patients_confirmed_and_suspected_covid_coverage, '') AS INTEGER)
		AS staffed_icu_adult_patients_confirmed_and_suspected_covid_coverage,
	CAST(NULLIF(staffed_icu_adult_patients_confirmed_covid, '') AS INTEGER)
		AS staffed_icu_adult_patients_confirmed_covid,
	CAST(NULLIF(staffed_icu_adult_patients_confirmed_covid_coverage, '') AS INTEGER)
		AS staffed_icu_adult_patients_confirmed_covid_coverage,
	CAST(NULLIF(total_staffed_adult_icu_beds, '') AS INTEGER)
		AS total_staffed_adult_icu_beds,
	CAST(NULLIF(total_staffed_adult_icu_beds_coverage, '') AS INTEGER)
		AS total_staffed_adult_icu_beds_coverage
FROM covid_hhs_sources.reported_hospital_utilization_timeseries
WHERE state = 'PR'
ORDER BY "$path" DESC, date DESC;

CREATE OR REPLACE VIEW covid_hhs_sources.reported_hospital_utilization_PR AS
SELECT
	date_parse(regexp_extract("$path", '202[012](\d{4})_(\d{4})'), '%Y%m%d_%H%i')
		AS file_timestamp,
	date(date_parse(regexp_extract("$path", '202[012](\d{4})_(\d{4})'), '%Y%m%d_%H%i'))
		AS date,
	CAST(NULLIF(inpatient_beds_used, '') AS INTEGER)
		AS inpatient_beds_used,
	CAST(NULLIF(inpatient_beds_used_coverage , '') AS INTEGER)
		AS inpatient_beds_used_coverage,
	CAST(NULLIF(inpatient_beds_used_covid, '') AS INTEGER)
		AS inpatient_beds_used_covid,
	CAST(NULLIF(inpatient_beds_used_covid_coverage, '') AS INTEGER)
		AS inpatient_beds_used_covid_coverage,
	CAST(NULLIF(previous_day_admission_adult_covid_confirmed, '') AS INTEGER)
		AS previous_day_admission_adult_covid_confirmed,
	CAST(NULLIF(previous_day_admission_adult_covid_confirmed_coverage, '') AS INTEGER)
		AS previous_day_admission_adult_covid_confirmed_coverage,
	CAST(NULLIF(previous_day_admission_adult_covid_suspected, '') AS INTEGER)
		AS previous_day_admission_adult_covid_suspected,
	CAST(NULLIF(previous_day_admission_adult_covid_suspected_coverage, '') AS INTEGER)
		AS previous_day_admission_adult_covid_suspected_coverage,
	CAST(NULLIF(previous_day_admission_pediatric_covid_confirmed, '') AS INTEGER)
		AS previous_day_admission_pediatric_covid_confirmed,
	CAST(NULLIF(previous_day_admission_pediatric_covid_confirmed_coverage, '') AS INTEGER)
		AS previous_day_admission_pediatric_covid_confirmed_coverage,
	CAST(NULLIF(previous_day_admission_pediatric_covid_suspected, '') AS INTEGER)
		AS previous_day_admission_pediatric_covid_suspected,
	CAST(NULLIF(previous_day_admission_pediatric_covid_suspected_coverage, '') AS INTEGER)
		AS previous_day_admission_pediatric_covid_suspected_coverage,
	CAST(NULLIF(staffed_adult_icu_bed_occupancy, '') AS INTEGER)
		AS staffed_adult_icu_bed_occupancy,
	CAST(NULLIF(staffed_adult_icu_bed_occupancy_coverage, '') AS INTEGER)
		AS staffed_adult_icu_bed_occupancy_coverage,
	CAST(NULLIF(staffed_icu_adult_patients_confirmed_and_suspected_covid, '') AS INTEGER)
		AS staffed_icu_adult_patients_confirmed_and_suspected_covid,
	CAST(NULLIF(staffed_icu_adult_patients_confirmed_and_suspected_covid_coverage, '') AS INTEGER)
		AS staffed_icu_adult_patients_confirmed_and_suspected_covid_coverage,
	CAST(NULLIF(staffed_icu_adult_patients_confirmed_covid, '') AS INTEGER)
		AS staffed_icu_adult_patients_confirmed_covid,
	CAST(NULLIF(staffed_icu_adult_patients_confirmed_covid_coverage, '') AS INTEGER)
		AS staffed_icu_adult_patients_confirmed_covid_coverage,
	CAST(NULLIF(total_staffed_adult_icu_beds, '') AS INTEGER)
		AS total_staffed_adult_icu_beds,
	CAST(NULLIF(total_staffed_adult_icu_beds_coverage, '') AS INTEGER)
		AS total_staffed_adult_icu_beds_coverage
FROM covid_hhs_sources.reported_hospital_utilization
WHERE state = 'PR'
ORDER BY "$path" DESC;

