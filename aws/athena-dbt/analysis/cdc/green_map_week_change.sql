--
-- Compare the most recent green map date to the previous one.
--
SELECT
	file_timestamp "Data from",
	curr.state "State",
	curr.county "County",
	curr.county_population "Population",
	cast(prev.covid_cases_per_100k AS VARCHAR)
		|| ' → '
		|| cast(curr.covid_cases_per_100k AS VARCHAR)
		AS "Case rate",
	cast(prev.covid_hospital_admissions_per_100k AS VARCHAR)
		|| ' → '
		|| cast(curr.covid_hospital_admissions_per_100k AS VARCHAR)
		AS "Admissions",
	cast(round(100.0 * prev.covid_inpatient_bed_utilization, 1) AS VARCHAR)
		|| ' → '
		|| cast(round(100.0 * curr.covid_inpatient_bed_utilization, 1) AS VARCHAR)
		AS "Bed %",
	prev.covid_19_community_level || ' → ' || curr.covid_19_community_level
		AS "Map level"
FROM {{ ref('cdc_community_level_all') }} curr
INNER JOIN {{ ref('cdc_community_level_all') }} prev
	USING (file_timestamp, county_fips)
WHERE file_timestamp = (
	SELECT max(file_timestamp)
	FROM {{ ref('cdc_community_level_all') }}
)
AND curr.date_updated = (
	SELECT max(date_updated)
	FROM {{ ref('cdc_community_level_all') }}
)
AND prev.date_updated = date_add('week', -1, curr.date_updated)
AND curr.covid_19_community_level = 'High'
AND prev.covid_19_community_level != 'High'
ORDER BY curr.county_population DESC;
