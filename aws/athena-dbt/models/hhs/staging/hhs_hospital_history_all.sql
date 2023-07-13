{{
    config(
        pre_hook=[
            "MSCK REPAIR TABLE {{ source('hhs', 'hospital_facilities_v4').render_hive() }}"
        ]
}}
SELECT
    {{ hhs_parse_filename_date('"$path"') }}
		AS file_timestamp,
	date(date_parse(collection_week, '%Y/%m/%d'))
		AS collection_week,
    CAST(state AS VARCHAR) AS state,
    {{ int_to_digits('fips_code', 5) }}
        AS fips_code,
    hospital_name,
    hospital_pk,
    all_adult_hospital_inpatient_beds_7_day_sum,
    all_adult_hospital_inpatient_beds_7_day_coverage,
    all_adult_hospital_inpatient_bed_occupied_7_day_sum,
    all_adult_hospital_inpatient_bed_occupied_7_day_coverage,
    total_adult_patients_hospitalized_confirmed_and_suspected_covid_7_day_sum,
    total_adult_patients_hospitalized_confirmed_and_suspected_covid_7_day_coverage,
    total_adult_patients_hospitalized_confirmed_covid_7_day_sum,
    total_adult_patients_hospitalized_confirmed_covid_7_day_coverage,
    total_pediatric_patients_hospitalized_confirmed_and_suspected_covid_7_day_sum,
    total_pediatric_patients_hospitalized_confirmed_covid_7_day_sum,
    total_pediatric_patients_hospitalized_confirmed_and_suspected_covid_7_day_coverage,
    total_pediatric_patients_hospitalized_confirmed_covid_7_day_coverage,
    total_staffed_adult_icu_beds_7_day_sum,
    total_staffed_adult_icu_beds_7_day_coverage,
    staffed_adult_icu_bed_occupancy_7_day_sum,
    staffed_adult_icu_bed_occupancy_7_day_coverage,
    staffed_icu_adult_patients_confirmed_and_suspected_covid_7_day_sum,
    staffed_icu_adult_patients_confirmed_and_suspected_covid_7_day_coverage,
    staffed_icu_adult_patients_confirmed_covid_7_day_sum,
    staffed_icu_adult_patients_confirmed_covid_7_day_coverage,
    previous_day_admission_adult_covid_confirmed_7_day_sum,
    previous_day_admission_pediatric_covid_confirmed_7_day_sum,
    previous_day_covid_ED_visits_7_day_sum,
    previous_day_admission_adult_covid_suspected_7_day_sum,
    previous_day_admission_pediatric_covid_suspected_7_day_sum,
    previous_day_total_ED_visits_7_day_sum,
    previous_day_admission_adult_covid_confirmed_7_day_coverage,
    previous_day_admission_pediatric_covid_confirmed_7_day_coverage,
    previous_day_admission_adult_covid_suspected_7_day_coverage,
    previous_day_admission_pediatric_covid_suspected_7_day_coverage
FROM {{ source('hhs', 'hospital_facilities_v3') }}

UNION ALL

SELECT
    file_timestamp,
	collection_week,
    CAST(state AS VARCHAR) AS state,
    fips_code,
    hospital_name,
    hospital_pk,
    all_adult_hospital_inpatient_beds_7_day_sum,
    all_adult_hospital_inpatient_beds_7_day_coverage,
    all_adult_hospital_inpatient_bed_occupied_7_day_sum,
    all_adult_hospital_inpatient_bed_occupied_7_day_coverage,
    total_adult_patients_hospitalized_confirmed_and_suspected_covid_7_day_sum,
    total_adult_patients_hospitalized_confirmed_and_suspected_covid_7_day_coverage,
    total_adult_patients_hospitalized_confirmed_covid_7_day_sum,
    total_adult_patients_hospitalized_confirmed_covid_7_day_coverage,
    total_pediatric_patients_hospitalized_confirmed_and_suspected_covid_7_day_sum,
    total_pediatric_patients_hospitalized_confirmed_covid_7_day_sum,
    total_pediatric_patients_hospitalized_confirmed_and_suspected_covid_7_day_coverage,
    total_pediatric_patients_hospitalized_confirmed_covid_7_day_coverage,
    total_staffed_adult_icu_beds_7_day_sum,
    total_staffed_adult_icu_beds_7_day_coverage,
    staffed_adult_icu_bed_occupancy_7_day_sum,
    staffed_adult_icu_bed_occupancy_7_day_coverage,
    staffed_icu_adult_patients_confirmed_and_suspected_covid_7_day_sum,
    staffed_icu_adult_patients_confirmed_and_suspected_covid_7_day_coverage,
    staffed_icu_adult_patients_confirmed_covid_7_day_sum,
    staffed_icu_adult_patients_confirmed_covid_7_day_coverage,
    previous_day_admission_adult_covid_confirmed_7_day_sum,
    previous_day_admission_pediatric_covid_confirmed_7_day_sum,
    previous_day_covid_ED_visits_7_day_sum,
    previous_day_admission_adult_covid_suspected_7_day_sum,
    previous_day_admission_pediatric_covid_suspected_7_day_sum,
    previous_day_total_ED_visits_7_day_sum,
    previous_day_admission_adult_covid_confirmed_7_day_coverage,
    previous_day_admission_pediatric_covid_confirmed_7_day_coverage,
    previous_day_admission_adult_covid_suspected_7_day_coverage,
    previous_day_admission_pediatric_covid_suspected_7_day_coverage
FROM {{ source('hhs', 'hospital_facilities_v4') }}