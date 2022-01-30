--
-- HHS hospitals data set
--

WITH max_timeseries_date AS (
	SELECT
		max(file_timestamp) AS max_file_timestamp,
		max(date) AS max_date
	FROM {{ source('hhs', 'hospital_utilization_timeseries') }}
)
SELECT hist.*
FROM {{ source('hhs', 'hospital_utilization_timeseries') }} hist
INNER JOIN max_timeseries_date
	ON file_timestamp = max_file_timestamp
UNION ALL
SELECT daily.*
FROM {{ source('hhs', 'hospital_utilization') }} daily
INNER JOIN max_timeseries_date
	ON date > max_date
ORDER BY date DESC;