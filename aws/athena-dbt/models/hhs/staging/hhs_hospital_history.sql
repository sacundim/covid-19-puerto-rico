WITH max_path AS (
    SELECT max("$path") max_path
    FROM {{ source('hhs', 'hospital_facilities') }}
)
SELECT
	date_parse(regexp_extract("$path", '202[012](\d{4})_(\d{4})'), '%Y%m%d_%H%i')
		AS file_timestamp,
	date(date_parse(collection_week, '%Y/%m/%d'))
		AS collection_week,
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
        AS staffed_icu_adult_patients_confirmed_covid_7_day_coverage
FROM {{ source('hhs', 'hospital_facilities') }}
INNER JOIN max_path
    ON max_path = "$path"
WHERE state = 'PR';