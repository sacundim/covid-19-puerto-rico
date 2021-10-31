SELECT
	week_start,
	week_end,
	next_week,
	region,
	sum(total_staffed_adult_icu_beds_7_day_lo)
		AS total_staffed_adult_icu_beds_7_day_lo,
	sum(LEAST(staffed_adult_icu_bed_occupancy_7_day_hi,
		 	  total_staffed_adult_icu_beds_7_day_lo))
		AS staffed_adult_icu_bed_occupancy_7_day_hi,
	sum(LEAST(staffed_icu_adult_patients_confirmed_and_suspected_covid_7_day_hi,
		      staffed_adult_icu_bed_occupancy_7_day_hi,
		      total_staffed_adult_icu_beds_7_day_lo))
	  AS staffed_icu_adult_patients_confirmed_and_suspected_covid_7_day_hi
FROM {{ ref('hhs_hospital_history_cube') }}
GROUP BY week_start, week_end, next_week, region
ORDER BY week_start DESC, region;
