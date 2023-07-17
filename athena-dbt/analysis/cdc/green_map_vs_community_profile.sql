--
-- Compare the CDC "green map" hospital admissions per 100k figures
-- with those implied by the county-level Community Profile Report,
-- for dates that are nominally the same for both.
--
{{ config(enabled=false) }}
SELECT
	green.date_updated "Date",
	green.state "State",
	green.health_service_area "Health Service Area",
	arbitrary(green.covid_hospital_admissions_per_100k)
		AS "Green Map admissions",
	arbitrary(1e5 * profile.confirmed_covid_hosp_last_7_days
		/ green.county_population)
		AS "CPR admissions"
FROM {{ ref('community_profile_report') }} profile
INNER JOIN {{ ref('cdc_community_level_all') }} green
	ON profile.fips = green.county_fips
	AND profile.date = green.date_updated
WHERE green.file_timestamp = (
	SELECT max(file_timestamp)
	FROM {{ ref('cdc_community_level_all') }}
)
GROUP BY
	green.date_updated,
	green.state,
	green.health_service_area
ORDER BY
    green.date_updated DESC,
    green.state,
    green.health_service_area;
