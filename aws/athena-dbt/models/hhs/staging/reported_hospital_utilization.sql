-------------------------------------------------------------------------
-------------------------------------------------------------------------
--
-- Minimal cleanup and type handling, plus filtering to Puerto Rico
--

SELECT
    {{ hhs_parse_filename_date('"$path"') }}
		AS file_timestamp,
	date({{ hhs_parse_filename_date('"$path"') }})
		AS date,
	CAST(NULLIF(hospital_onset_covid, '') AS INTEGER)
		AS hospital_onset_covid,
	CAST(NULLIF(hospital_onset_covid_coverage , '') AS INTEGER)
		AS hospital_onset_covid_coverage,
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