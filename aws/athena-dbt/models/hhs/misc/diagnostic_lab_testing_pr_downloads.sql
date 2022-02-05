SELECT
	date_parse(regexp_extract("$path", '202[012](\d{4})_(\d{4})'), '%Y%m%d_%H%i')
		AS file_timestamp,
	date(date_parse(date, '%Y/%m/%d')) AS date,
	state,
	state_fips,
	overall_outcome,
	CAST(NULLIF(new_results_reported, '') AS INTEGER)
		AS new_results_reported,
	CAST(NULLIF(total_results_reported, '') AS INTEGER)
		AS total_results_reported
FROM {{ source('hhs', 'diagnostic_lab_testing' )}}
WHERE state = 'PR'
ORDER BY file_timestamp, date;
