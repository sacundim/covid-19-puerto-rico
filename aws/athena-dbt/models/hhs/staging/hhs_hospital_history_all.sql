WITH max_path AS (
    SELECT max("$path") max_path
    FROM {{ source('hhs', 'hospital_facilities') }}
)
SELECT
    {{ hhs_parse_filename_date('"$path"') }}
		AS file_timestamp,
	date(date_parse(collection_week, '%Y/%m/%d'))
		AS collection_week,
    state,
    fips_code,
    hospital_name,
    hospital_pk,
    CAST(nullif(all_adult_hospital_inpatient_beds_7_day_sum, '') AS INT)
    	AS all_adult_hospital_inpatient_beds_7_day_sum,
    CAST(nullif(all_adult_hospital_inpatient_beds_7_day_coverage, '') AS INT)
        AS all_adult_hospital_inpatient_beds_7_day_coverage,
    CAST(nullif(all_adult_hospital_inpatient_bed_occupied_7_day_sum, '') AS INT)
        AS all_adult_hospital_inpatient_bed_occupied_7_day_sum,
    CAST(nullif(all_adult_hospital_inpatient_bed_occupied_7_day_coverage, '') AS INT)
        AS all_adult_hospital_inpatient_bed_occupied_7_day_coverage,
    CAST(nullif(total_adult_patients_hospitalized_confirmed_and_suspected_covid_7_day_sum, '') AS INT)
        AS total_adult_patients_hospitalized_confirmed_and_suspected_covid_7_day_sum,
    CAST(nullif(total_adult_patients_hospitalized_confirmed_and_suspected_covid_7_day_coverage, '') AS INT)
        AS total_adult_patients_hospitalized_confirmed_and_suspected_covid_7_day_coverage,
    CAST(nullif(total_adult_patients_hospitalized_confirmed_covid_7_day_sum, '') AS INT)
        AS total_adult_patients_hospitalized_confirmed_covid_7_day_sum,
    CAST(nullif(total_adult_patients_hospitalized_confirmed_covid_7_day_coverage, '') AS INT)
        AS total_adult_patients_hospitalized_confirmed_covid_7_day_coverage,
    {{ cast_string_column('total_pediatric_patients_hospitalized_confirmed_and_suspected_covid_7_day_sum', 'INT') }},
    {{ cast_string_column('total_pediatric_patients_hospitalized_confirmed_covid_7_day_sum', 'INT') }},
    {{ cast_string_column('total_pediatric_patients_hospitalized_confirmed_and_suspected_covid_7_day_coverage', 'INT') }},
    {{ cast_string_column('total_pediatric_patients_hospitalized_confirmed_covid_7_day_coverage', 'INT') }},
    CAST(nullif(total_staffed_adult_icu_beds_7_day_sum, '') AS INT)
        AS total_staffed_adult_icu_beds_7_day_sum,
    CAST(nullif(total_staffed_adult_icu_beds_7_day_coverage, '') AS INT)
        AS total_staffed_adult_icu_beds_7_day_coverage,
    CAST(nullif(staffed_adult_icu_bed_occupancy_7_day_sum, '') AS INT)
        AS staffed_adult_icu_bed_occupancy_7_day_sum,
    CAST(nullif(staffed_adult_icu_bed_occupancy_7_day_coverage, '') AS INT)
        AS staffed_adult_icu_bed_occupancy_7_day_coverage,
    CAST(nullif(staffed_icu_adult_patients_confirmed_and_suspected_covid_7_day_sum, '') AS INT)
        AS staffed_icu_adult_patients_confirmed_and_suspected_covid_7_day_sum,
    CAST(nullif(staffed_icu_adult_patients_confirmed_and_suspected_covid_7_day_coverage, '') AS INT)
        AS staffed_icu_adult_patients_confirmed_and_suspected_covid_7_day_coverage,
    CAST(nullif(staffed_icu_adult_patients_confirmed_covid_7_day_sum, '') AS INT)
        AS staffed_icu_adult_patients_confirmed_covid_7_day_sum,
    CAST(nullif(staffed_icu_adult_patients_confirmed_covid_7_day_coverage, '') AS INT)
        AS staffed_icu_adult_patients_confirmed_covid_7_day_coverage,
    {{ cast_string_column('previous_day_admission_adult_covid_confirmed_7_day_sum', 'INT') }},
    {{ cast_string_column('previous_day_admission_pediatric_covid_confirmed_7_day_sum', 'INT') }},
    {{ cast_string_column('previous_day_covid_ED_visits_7_day_sum', 'INT') }},
    {{ cast_string_column('previous_day_admission_adult_covid_suspected_7_day_sum', 'INT') }},
    {{ cast_string_column('previous_day_admission_pediatric_covid_suspected_7_day_sum', 'INT') }},
    {{ cast_string_column('previous_day_total_ED_visits_7_day_sum', 'INT') }},
    {{ cast_string_column('previous_day_admission_adult_covid_confirmed_7_day_coverage', 'INT') }},
    {{ cast_string_column('previous_day_admission_pediatric_covid_confirmed_7_day_coverage', 'INT') }},
    {{ cast_string_column('previous_day_admission_adult_covid_suspected_7_day_coverage', 'INT') }},
    {{ cast_string_column('previous_day_admission_pediatric_covid_suspected_7_day_coverage', 'INT') }}
FROM {{ source('hhs', 'hospital_facilities') }}
INNER JOIN max_path
    ON max_path = "$path";