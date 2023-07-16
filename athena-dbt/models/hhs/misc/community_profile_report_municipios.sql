SELECT
	date(date_parse(date, '%m/%d/%Y %h:%i:%s %p')) AS date,
	fips,
	county,
	CAST(NULLIF(total_tests_last_7_days, '') AS INTEGER)
		AS total_tests_last_7_days,
	CAST(NULLIF(total_positive_tests_last_7_days, '') AS INTEGER)
		AS total_positive_tests_last_7_days
FROM {{ source('hhs', 'community_profile_report_county_v2') }}
WHERE state = 'PR'