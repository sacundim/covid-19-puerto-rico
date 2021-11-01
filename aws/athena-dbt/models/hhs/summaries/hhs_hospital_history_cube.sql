SELECT
	collection_week AS week_start,
	date_add('day', 6, collection_week) AS week_end,
	date_add('day', 7, collection_week) AS next_week,
	cmn.region region,
	cmn."name" municipality,
	fips_code,
	hospital_name,
	hospital_pk,

	{{ hhs_avg('all_adult_hospital_inpatient_beds_7_day_sum',
	        'all_adult_hospital_inpatient_beds_7_day_coverage') }}
		AS all_adult_hospital_inpatient_beds_7_day_avg,
	{{ hhs_lo('all_adult_hospital_inpatient_beds_7_day_sum',
	       'all_adult_hospital_inpatient_beds_7_day_coverage') }}
		AS all_adult_hospital_inpatient_beds_7_day_lo,
	{{ hhs_hi('all_adult_hospital_inpatient_beds_7_day_sum',
	       'all_adult_hospital_inpatient_beds_7_day_coverage') }}
		AS all_adult_hospital_inpatient_beds_7_day_hi,

	{{ hhs_avg('all_adult_hospital_inpatient_bed_occupied_7_day_sum',
	        'all_adult_hospital_inpatient_bed_occupied_7_day_coverage') }}
		AS all_adult_hospital_inpatient_bed_occupied_7_day_avg,
	{{ hhs_lo('all_adult_hospital_inpatient_bed_occupied_7_day_sum',
	       'all_adult_hospital_inpatient_bed_occupied_7_day_coverage') }}
		AS all_adult_hospital_inpatient_bed_occupied_7_day_lo,
	{{ hhs_hi('all_adult_hospital_inpatient_bed_occupied_7_day_sum',
	       'all_adult_hospital_inpatient_bed_occupied_7_day_coverage') }}
		AS all_adult_hospital_inpatient_bed_occupied_7_day_hi,

	{{ hhs_avg('total_adult_patients_hospitalized_confirmed_and_suspected_covid_7_day_sum',
	        'total_adult_patients_hospitalized_confirmed_and_suspected_covid_7_day_coverage') }}
		AS total_adult_patients_hospitalized_confirmed_and_suspected_covid_7_day_avg,
	{{ hhs_lo('total_adult_patients_hospitalized_confirmed_and_suspected_covid_7_day_sum',
	       'total_adult_patients_hospitalized_confirmed_and_suspected_covid_7_day_coverage') }}
		AS total_adult_patients_hospitalized_confirmed_and_suspected_covid_7_day_lo,
	{{ hhs_hi('total_adult_patients_hospitalized_confirmed_and_suspected_covid_7_day_sum',
	       'total_adult_patients_hospitalized_confirmed_and_suspected_covid_7_day_coverage') }}
		AS total_adult_patients_hospitalized_confirmed_and_suspected_covid_7_day_hi,

	{{ hhs_avg('total_adult_patients_hospitalized_confirmed_covid_7_day_sum',
	        'total_adult_patients_hospitalized_confirmed_covid_7_day_coverage') }}
		AS total_adult_patients_hospitalized_confirmed_covid_7_day_avg,
	{{ hhs_lo('total_adult_patients_hospitalized_confirmed_covid_7_day_sum',
	       'total_adult_patients_hospitalized_confirmed_covid_7_day_coverage') }}
		AS total_adult_patients_hospitalized_confirmed_covid_7_day_lo,
	{{ hhs_hi('total_adult_patients_hospitalized_confirmed_covid_7_day_sum',
	       'total_adult_patients_hospitalized_confirmed_covid_7_day_coverage') }}
		AS total_adult_patients_hospitalized_confirmed_covid_7_day_hi,

	{{ hhs_avg('total_staffed_adult_icu_beds_7_day_sum',
	        'total_staffed_adult_icu_beds_7_day_coverage') }}
		AS total_staffed_adult_icu_beds_7_day_avg,
	{{ hhs_lo('total_staffed_adult_icu_beds_7_day_sum',
	       'total_staffed_adult_icu_beds_7_day_coverage') }}
		AS total_staffed_adult_icu_beds_7_day_lo,
	{{ hhs_hi('total_staffed_adult_icu_beds_7_day_sum',
	       'total_staffed_adult_icu_beds_7_day_coverage') }}
		AS total_staffed_adult_icu_beds_7_day_hi,

	{{ hhs_avg('staffed_adult_icu_bed_occupancy_7_day_sum',
	        'staffed_adult_icu_bed_occupancy_7_day_coverage') }}
		AS staffed_adult_icu_bed_occupancy_7_day_avg,
	{{ hhs_lo('staffed_adult_icu_bed_occupancy_7_day_sum',
	       'staffed_adult_icu_bed_occupancy_7_day_coverage') }}
		AS staffed_adult_icu_bed_occupancy_7_day_lo,
	{{ hhs_hi('staffed_adult_icu_bed_occupancy_7_day_sum',
	       'staffed_adult_icu_bed_occupancy_7_day_coverage') }}
		AS staffed_adult_icu_bed_occupancy_7_day_hi,

	{{ hhs_avg('staffed_icu_adult_patients_confirmed_and_suspected_covid_7_day_sum',
	        'staffed_icu_adult_patients_confirmed_and_suspected_covid_7_day_coverage') }}
		AS staffed_icu_adult_patients_confirmed_and_suspected_covid_7_day_avg,
	{{ hhs_lo('staffed_icu_adult_patients_confirmed_and_suspected_covid_7_day_sum',
	       'staffed_icu_adult_patients_confirmed_and_suspected_covid_7_day_coverage') }}
		AS staffed_icu_adult_patients_confirmed_and_suspected_covid_7_day_lo,
	{{ hhs_hi('staffed_icu_adult_patients_confirmed_and_suspected_covid_7_day_sum',
	       'staffed_icu_adult_patients_confirmed_and_suspected_covid_7_day_coverage') }}
		AS staffed_icu_adult_patients_confirmed_and_suspected_covid_7_day_hi,

	{{ hhs_avg('staffed_icu_adult_patients_confirmed_covid_7_day_sum',
	        'staffed_icu_adult_patients_confirmed_covid_7_day_coverage') }}
		AS staffed_icu_adult_patients_confirmed_covid_7_day_avg,
	{{ hhs_lo('staffed_icu_adult_patients_confirmed_covid_7_day_sum',
	       'staffed_icu_adult_patients_confirmed_covid_7_day_coverage') }}
		AS staffed_icu_adult_patients_confirmed_covid_7_day_lo,
	{{ hhs_hi('staffed_icu_adult_patients_confirmed_covid_7_day_sum',
	       'staffed_icu_adult_patients_confirmed_covid_7_day_coverage') }}
		AS staffed_icu_adult_patients_confirmed_covid_7_day_hi
FROM {{ ref('hhs_hospital_history') }} hhh
INNER JOIN {{ ref('municipal_population') }} cmn
	ON cmn.fips = hhh.fips_code
ORDER BY collection_week DESC, region, cmn."name", hospital_name;
