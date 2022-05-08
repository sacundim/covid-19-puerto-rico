-------------------------------------------------------------------------
-------------------------------------------------------------------------
--
-- Minimal cleanup and type handling
--

SELECT
    {{ hhs_parse_filename_date('"$path"') }}
		AS file_timestamp,
	date(date_parse(date, '%Y/%m/%d')) AS date,
	state,
    {{ cast_string_column('hospital_onset_covid', 'INTEGER') }},
    {{ cast_string_column('hospital_onset_covid_coverage', 'INTEGER') }},
    {{ cast_string_column('inpatient_beds', 'INTEGER') }},
    {{ cast_string_column('inpatient_beds_coverage', 'INTEGER') }},
    {{ cast_string_column('inpatient_beds_used', 'INTEGER') }},
    {{ cast_string_column('inpatient_beds_used_coverage', 'INTEGER') }},
    {{ cast_string_column('inpatient_beds_used_covid', 'INTEGER') }},
    {{ cast_string_column('inpatient_beds_used_covid_coverage', 'INTEGER') }},
    {{ cast_string_column('previous_day_admission_adult_covid_confirmed', 'INTEGER') }},
    {{ cast_string_column('previous_day_admission_adult_covid_confirmed_coverage', 'INTEGER') }},
    {{ cast_string_column('previous_day_admission_adult_covid_suspected', 'INTEGER') }},
    {{ cast_string_column('previous_day_admission_adult_covid_suspected_coverage', 'INTEGER') }},
    {{ cast_string_column('previous_day_admission_pediatric_covid_confirmed', 'INTEGER') }},
    {{ cast_string_column('previous_day_admission_pediatric_covid_confirmed_coverage', 'INTEGER') }},
    {{ cast_string_column('previous_day_admission_pediatric_covid_suspected', 'INTEGER') }},
    {{ cast_string_column('previous_day_admission_pediatric_covid_suspected_coverage', 'INTEGER') }},
    {{ cast_string_column('staffed_adult_icu_bed_occupancy', 'INTEGER') }},
    {{ cast_string_column('staffed_adult_icu_bed_occupancy_coverage', 'INTEGER') }},
    {{ cast_string_column('staffed_icu_adult_patients_confirmed_and_suspected_covid', 'INTEGER') }},
    {{ cast_string_column('staffed_icu_adult_patients_confirmed_and_suspected_covid_coverage', 'INTEGER') }},
    {{ cast_string_column('staffed_icu_adult_patients_confirmed_covid', 'INTEGER') }},
    {{ cast_string_column('staffed_icu_adult_patients_confirmed_covid_coverage', 'INTEGER') }},
    {{ cast_string_column('total_staffed_adult_icu_beds', 'INTEGER') }},
    {{ cast_string_column('total_staffed_adult_icu_beds_coverage', 'INTEGER') }},
    {{ cast_string_column('total_adult_patients_hospitalized_confirmed_and_suspected_covid', 'INTEGER') }},
    {{ cast_string_column('total_adult_patients_hospitalized_confirmed_and_suspected_covid_coverage', 'INTEGER') }},
    {{ cast_string_column('total_adult_patients_hospitalized_confirmed_covid', 'INTEGER') }},
    {{ cast_string_column('total_adult_patients_hospitalized_confirmed_covid_coverage', 'INTEGER') }},
    {{ cast_string_column('total_pediatric_patients_hospitalized_confirmed_and_suspected_covid', 'INTEGER') }},
    {{ cast_string_column('total_pediatric_patients_hospitalized_confirmed_and_suspected_covid_coverage', 'INTEGER') }},
    {{ cast_string_column('total_pediatric_patients_hospitalized_confirmed_covid', 'INTEGER') }},
    {{ cast_string_column('total_pediatric_patients_hospitalized_confirmed_covid_coverage', 'INTEGER') }}
FROM {{ source('hhs', 'reported_hospital_utilization_timeseries') }}
ORDER BY "$path", date, state