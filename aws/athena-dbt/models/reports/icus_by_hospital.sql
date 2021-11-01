SELECT
	week_start,
	week_end,
	next_week,
	hospital_name,
	municipality,
	total_staffed_adult_icu_beds_7_day_lo,
	-- Occupied ICU beds can't be more than staffed ones:
	LEAST(staffed_adult_icu_bed_occupancy_7_day_hi,
		  total_staffed_adult_icu_beds_7_day_lo)
		AS staffed_adult_icu_bed_occupancy_7_day_hi,
	-- ICU COVID patients can't be more than either occupied
	-- or staffed beds:
	LEAST(staffed_icu_adult_patients_confirmed_and_suspected_covid_7_day_hi,
		  staffed_adult_icu_bed_occupancy_7_day_hi,
		  total_staffed_adult_icu_beds_7_day_lo)
	  AS staffed_icu_adult_patients_confirmed_and_suspected_covid_7_day_hi
FROM {{ ref('hhs_hospital_history_cube') }}
ORDER BY week_start DESC, hospital_name;
