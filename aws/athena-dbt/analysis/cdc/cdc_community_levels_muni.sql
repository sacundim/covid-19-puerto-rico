WITH green_agg AS (
	SELECT
		-- We are going to use this max to pick out the most recent version
		-- of the CDC Levels file
		max(file_timestamp) AS file_timestamp,
		-- Use Puerto Rico population according to the CDC Levels file
		max(health_service_area_population) AS popest2019
	FROM {{ ref('cdc_community_level' )}}
), hhs AS (
	WITH bitemporal AS (
		SELECT
			bulletin_date,
			date,
			1e5 * sum(previous_day_admission_covid_confirmed) OVER (
				PARTITION BY bulletin_date
				ORDER BY date
				ROWS 6 PRECEDING
			) / green_agg.popest2019 AS admission_rate,
			100.0 * avg(total_patients_hospitalized_confirmed_covid) OVER (
				PARTITION BY bulletin_date
				ORDER BY date
				ROWS 6 PRECEDING
			) / avg(inpatient_beds) OVER (
				PARTITION BY bulletin_date
				ORDER BY date
				ROWS 6 PRECEDING
			) AS occupancy_pct
		FROM {{ ref('hhs_hospitals_bitemporal') }}
		CROSS JOIN green_agg
	)
	SELECT *
	FROM bitemporal
	WHERE bulletin_date = date
)
SELECT
	muni.bulletin_date AS "Data up to",
	municipality "City",
	1e5 * sum(delta_cases) OVER (
		PARTITION BY fips
		ORDER BY muni.bulletin_date
		ROWS 6 PRECEDING
	) / muni.popest2019 AS "Cases (PRDoH)",
	green.covid_cases_per_100k AS "Green map",
	hhs.admission_rate AS "Admission rate (HHS)",
	green.covid_hospital_admissions_per_100k "Green map",
	hhs.occupancy_pct AS "Hospital occupancy (HHS)",
	100.0 * green.covid_inpatient_bed_utilization
		AS "Green map"
FROM {{ ref('cases_municipal_reported_agg') }} muni
INNER JOIN hhs
	ON hhs.bulletin_date = muni.bulletin_date
CROSS JOIN green_agg
	ON green.date_updated = muni.bulletin_date
		AND green.county_fips = muni.fips
		-- Use most recent version of CDC Levels file:
		AND green.file_timestamp  = green_agg.file_timestamp
WHERE municipality = 'San Juan'
ORDER BY muni.bulletin_date DESC, fips;