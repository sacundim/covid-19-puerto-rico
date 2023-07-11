-------------------------------------------------------------------------
-------------------------------------------------------------------------
--
-- Minimal cleanup and type handling
--

SELECT
    -- Edge case: cutover between two versions of the storage can
    -- lead (and has led) to two copies of the same file
    "$path" s3_path,

    {{ hhs_parse_filename_date('"$path"') }}
		AS file_timestamp,
	date(date_parse(date, '%m/%d/%Y %h:%i:%s %p')) AS date,
	state,
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
    total_staffed_adult_icu_beds_coverage,
    total_adult_patients_hospitalized_confirmed_and_suspected_covid,
    total_adult_patients_hospitalized_confirmed_and_suspected_covid_coverage,
    total_adult_patients_hospitalized_confirmed_covid,
    total_adult_patients_hospitalized_confirmed_covid_coverage,
    total_pediatric_patients_hospitalized_confirmed_and_suspected_covid,
    total_pediatric_patients_hospitalized_confirmed_and_suspected_covid_coverage,
    total_pediatric_patients_hospitalized_confirmed_covid,
    total_pediatric_patients_hospitalized_confirmed_covid_coverage
FROM {{ source('hhs', 'reported_patient_impact_hospital_capacity_timeseries_v3') }}

UNION ALL

SELECT
    -- Edge case: cutover between two versions of the storage can
    -- lead (and has led) to two copies of the same file
    "$path" s3_path,

    {{ hhs_parse_filename_date('"$path"') }}
		AS file_timestamp,
	date(date_parse(date, '%Y/%m/%d')) AS date,
	state,
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
    total_staffed_adult_icu_beds_coverage,
    total_adult_patients_hospitalized_confirmed_and_suspected_covid,
    total_adult_patients_hospitalized_confirmed_and_suspected_covid_coverage,
    total_adult_patients_hospitalized_confirmed_covid,
    total_adult_patients_hospitalized_confirmed_covid_coverage,
    total_pediatric_patients_hospitalized_confirmed_and_suspected_covid,
    total_pediatric_patients_hospitalized_confirmed_and_suspected_covid_coverage,
    total_pediatric_patients_hospitalized_confirmed_covid,
    total_pediatric_patients_hospitalized_confirmed_covid_coverage
FROM {{ source('hhs', 'reported_hospital_utilization_timeseries_v3') }}

UNION ALL

SELECT
    -- Edge case: cutover between two versions of the storage can
    -- lead (and has led) to two copies of the same file at different
    -- paths
    "$path" s3_path,

    {{ hhs_parse_filename_date('"$path"') }}
		AS file_timestamp,
	date(date_parse(date, '%Y/%m/%d')) AS date,
	state,
	-- BIGINTs because that's what V3 above gives us:
    {{ cast_string_column('hospital_onset_covid', 'BIGINT') }},
    {{ cast_string_column('hospital_onset_covid_coverage', 'BIGINT') }},
    {{ cast_string_column('inpatient_beds', 'BIGINT') }},
    {{ cast_string_column('inpatient_beds_coverage', 'BIGINT') }},
    {{ cast_string_column('inpatient_beds_used', 'BIGINT') }},
    {{ cast_string_column('inpatient_beds_used_coverage', 'BIGINT') }},
    {{ cast_string_column('inpatient_beds_used_covid', 'BIGINT') }},
    {{ cast_string_column('inpatient_beds_used_covid_coverage', 'BIGINT') }},
    {{ cast_string_column('previous_day_admission_adult_covid_confirmed', 'BIGINT') }},
    {{ cast_string_column('previous_day_admission_adult_covid_confirmed_coverage', 'BIGINT') }},
    {{ cast_string_column('previous_day_admission_adult_covid_suspected', 'BIGINT') }},
    {{ cast_string_column('previous_day_admission_adult_covid_suspected_coverage', 'BIGINT') }},
    {{ cast_string_column('previous_day_admission_pediatric_covid_confirmed', 'BIGINT') }},
    {{ cast_string_column('previous_day_admission_pediatric_covid_confirmed_coverage', 'BIGINT') }},
    {{ cast_string_column('previous_day_admission_pediatric_covid_suspected', 'BIGINT') }},
    {{ cast_string_column('previous_day_admission_pediatric_covid_suspected_coverage', 'BIGINT') }},
    {{ cast_string_column('staffed_adult_icu_bed_occupancy', 'BIGINT') }},
    {{ cast_string_column('staffed_adult_icu_bed_occupancy_coverage', 'BIGINT') }},
    {{ cast_string_column('staffed_icu_adult_patients_confirmed_and_suspected_covid', 'BIGINT') }},
    {{ cast_string_column('staffed_icu_adult_patients_confirmed_and_suspected_covid_coverage', 'BIGINT') }},
    {{ cast_string_column('staffed_icu_adult_patients_confirmed_covid', 'BIGINT') }},
    {{ cast_string_column('staffed_icu_adult_patients_confirmed_covid_coverage', 'BIGINT') }},
    {{ cast_string_column('total_staffed_adult_icu_beds', 'BIGINT') }},
    {{ cast_string_column('total_staffed_adult_icu_beds_coverage', 'BIGINT') }},
    {{ cast_string_column('total_adult_patients_hospitalized_confirmed_and_suspected_covid', 'BIGINT') }},
    {{ cast_string_column('total_adult_patients_hospitalized_confirmed_and_suspected_covid_coverage', 'BIGINT') }},
    {{ cast_string_column('total_adult_patients_hospitalized_confirmed_covid', 'BIGINT') }},
    {{ cast_string_column('total_adult_patients_hospitalized_confirmed_covid_coverage', 'BIGINT') }},
    {{ cast_string_column('total_pediatric_patients_hospitalized_confirmed_and_suspected_covid', 'BIGINT') }},
    {{ cast_string_column('total_pediatric_patients_hospitalized_confirmed_and_suspected_covid_coverage', 'BIGINT') }},
    {{ cast_string_column('total_pediatric_patients_hospitalized_confirmed_covid', 'BIGINT') }},
    {{ cast_string_column('total_pediatric_patients_hospitalized_confirmed_covid_coverage', 'BIGINT') }}
FROM {{ source('hhs', 'reported_hospital_utilization_timeseries_v2') }}