-------------------------------------------------------------------------
-------------------------------------------------------------------------
--
-- Minimal cleanup and type handling, plus filtering to Puerto Rico
--
{{ config(enabled=false) }}
SELECT
    {{ hhs_parse_filename_date('"$path"') }}
		AS file_timestamp,
	date({{ hhs_parse_filename_date('"$path"') }})
		AS date,
	hospital_onset_covid,
	hospital_onset_covid_coverage,
	inpatient_beds,
	inpatient_beds_coverage,
	inpatient_beds_used,
	inpatient_beds_used_coverage,
	inpatient_beds_used_covid,
	inpatient_beds_used_covid_coverage,
	previous_day_admission_adult_covid_confirmed,
	previous_day_admission_adult_covid_confirmed_coverage,
	previous_day_admission_adult_covid_suspected,
	previous_day_admission_adult_covid_suspected_coverage,
	previous_day_admission_pediatric_covid_confirmed,
	previous_day_admission_pediatric_covid_confirmed_coverage,
	previous_day_admission_pediatric_covid_suspected,
	previous_day_admission_pediatric_covid_suspected_coverage,
	staffed_adult_icu_bed_occupancy,
	staffed_adult_icu_bed_occupancy_coverage,
	staffed_icu_adult_patients_confirmed_and_suspected_covid,
	staffed_icu_adult_patients_confirmed_and_suspected_covid_coverage,
	staffed_icu_adult_patients_confirmed_covid,
	staffed_icu_adult_patients_confirmed_covid_coverage,
	total_staffed_adult_icu_beds,
	total_staffed_adult_icu_beds_coverage
FROM {{ source('hhs', 'reported_hospital_utilization_v3') }}
WHERE state = 'PR'

UNION ALL

SELECT
    {{ hhs_parse_filename_date('"$path"') }}
		AS file_timestamp,
	date({{ hhs_parse_filename_date('"$path"') }})
		AS date,
	CAST(NULLIF(hospital_onset_covid, '') AS INTEGER)
		AS hospital_onset_covid,
	CAST(NULLIF(hospital_onset_covid_coverage , '') AS INTEGER)
		AS hospital_onset_covid_coverage,
	CAST(NULLIF(inpatient_beds, '') AS INTEGER)
		AS inpatient_beds,
	CAST(NULLIF(inpatient_beds_coverage , '') AS INTEGER)
		AS inpatient_beds_coverage,
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
FROM {{ source('hhs', 'reported_hospital_utilization_v2') }}
WHERE state = 'PR'