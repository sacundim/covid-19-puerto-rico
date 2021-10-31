--
-- HHS hospitals data set
--

WITH max_timeseries_date AS (
	SELECT
		max(file_timestamp) AS max_file_timestamp,
		max(date) AS max_date
	FROM covid_hhs_sources.reported_hospital_utilization_timeseries_PR
)
SELECT hist.*
FROM covid_hhs_sources.reported_hospital_utilization_timeseries_PR hist
INNER JOIN max_timeseries_date
	ON file_timestamp = max_file_timestamp
UNION ALL
SELECT daily.*
FROM covid_hhs_sources.reported_hospital_utilization_PR daily
INNER JOIN max_timeseries_date
	ON date > max_date
ORDER BY date DESC;
