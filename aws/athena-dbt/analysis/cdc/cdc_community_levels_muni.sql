WITH green_agg AS (
	SELECT
		-- We are going to use this max to pick out the most recent version
		-- of the CDC Levels file
		max(file_timestamp) AS file_timestamp,
		-- Use Puerto Rico population according to the CDC Levels file
		max(health_service_area_population) AS popest2019
	FROM {{ ref('cdc_community_level' )}}
)
SELECT
	muni.bulletin_date AS "Data up to",
	municipality "Municipality",
	1e5 * sum(delta_cases) OVER (
		PARTITION BY fips
		ORDER BY muni.bulletin_date
		ROWS 6 PRECEDING
	) / muni.popest2019 AS "Case rate (PRDoH)",
	green.covid_cases_per_100k AS "(CDC)",
	1e5 * sum(previous_day_admission_covid_confirmed) OVER (
		PARTITION BY fips
		ORDER BY muni.bulletin_date
		ROWS 6 PRECEDING
	) / green_agg.popest2019 AS "Admission rate (HHS)",
	green.covid_hospital_admissions_per_100k "(CDC)",
	100.0 * avg(total_patients_hospitalized_confirmed_covid) OVER (
		PARTITION BY fips
		ORDER BY muni.bulletin_date
		ROWS 6 PRECEDING
	) / avg(inpatient_beds) OVER (
		PARTITION BY fips
		ORDER BY muni.bulletin_date
		ROWS 6 PRECEDING
	) AS "Hospital occupancy (HHS)",
	100.0 * green.covid_inpatient_bed_utilization
		AS "(CDC)"
FROM {{ ref('cases_municipal_reported_agg') }} muni
INNER JOIN {{ ref('hhs_hospitals_bitemporal') }} hospitals
	ON hospitals.bulletin_date = muni.bulletin_date
		AND hospitals.bulletin_date = hospitals."date"
CROSS JOIN green_agg
	ON green.date_updated = muni.bulletin_date
		AND green.county_fips = muni.fips
		-- Use most reent version of CDC Levels file:
		AND green.file_timestamp  = green_agg.file_timestamp
WHERE municipality = 'San Juan'
ORDER BY muni.bulletin_date DESC, fips;