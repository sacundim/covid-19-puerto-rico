SELECT
	CAST(from_iso8601_timestamp(
		regexp_extract("$path", '(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})Z'))
		AS TIMESTAMP)
		AS downloaded_at,
	*
FROM {{ source('hhs', 'vaccination_county_condensed_data_json') }};