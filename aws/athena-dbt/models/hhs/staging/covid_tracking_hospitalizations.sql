SELECT
	CAST(date AS DATE) AS date,
	CAST(NULLIF(hospitalizedCurrently, '') AS INTEGER)
		AS hospitalized_currently,
	CAST(NULLIF(inIcuCurrently, '') AS INTEGER)
		AS in_icu_currently
FROM {{ source('hhs', 'covid_tracking_csv') }}
WHERE state = 'PR';
