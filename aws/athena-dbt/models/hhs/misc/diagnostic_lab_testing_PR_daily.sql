WITH downloads AS (
	SELECT
		date(file_timestamp AT TIME ZONE 'America/New_York') local_date,
		max(file_timestamp) AS max_file_timestamp
	FROM covid_hhs_sources.diagnostic_lab_testing_PR_downloads
	GROUP BY date(file_timestamp AT TIME ZONE 'America/New_York')
)
SELECT
	file_timestamp,
	local_date AS bulletin_date,
	date,
	sum(new_results_reported) AS tests,
	sum(new_results_reported) FILTER (
		WHERE overall_outcome IN ('Positive', 'Negative')
	) AS conclusive,
	sum(new_results_reported) FILTER (
		WHERE overall_outcome = 'Positive'
	) AS positive,
	sum(sum(new_results_reported)) OVER (
		PARTITION BY file_timestamp
		ORDER BY date
	) AS cumulative_tests,
	sum(sum(new_results_reported) FILTER (
		WHERE overall_outcome IN ('Positive', 'Negative')
	)) OVER (
		PARTITION BY file_timestamp
		ORDER BY date
	) AS cumulative_conclusive,
	sum(sum(new_results_reported) FILTER (
		WHERE overall_outcome = 'Positive'
	)) OVER (
		PARTITION BY file_timestamp
		ORDER BY date
	) AS cumulative_positive
FROM {{ ref('diagnostic_lab_testing_PR_downloads') }}
INNER JOIN downloads
	ON max_file_timestamp = file_timestamp
GROUP BY file_timestamp, local_date, date
ORDER BY file_timestamp, local_date, date;
