--
-- Head start on an analysis of CDC testing dataset bitemporal revisions
--
WITH raw AS (
	SELECT
		regexp_extract("$path", 'covid-19_diagnostic_lab_testing_[0-9]+_[0-9]+\.parquet$')
			AS filename,
		date_parse(regexp_extract("$path", '202[0123](\d{4})_(\d{4})'), '%Y%m%d_%H%i')
			AS file_datetime,
		date(date_parse(date, '%Y/%m/%d')) date,
		sum(new_results_reported) new_results_reported,
		sum(new_results_reported) FILTER (
			WHERE overall_outcome = 'Positive'
		) AS new_positives_reported,
		sum(sum(new_results_reported)) OVER (
			PARTITION BY "$path"
			ORDER BY date
			ROWS 6 PRECEDING
		) AS new_results_reported_7d,
		sum(sum(new_results_reported) FILTER (
			WHERE overall_outcome != 'Inconclusive'
		)) OVER (
			PARTITION BY "$path"
			ORDER BY date
			ROWS 6 PRECEDING
		) AS new_conclusives_reported_7d,
		sum(sum(new_results_reported) FILTER (
			WHERE overall_outcome = 'Positive'
		)) OVER (
			PARTITION BY "$path"
			ORDER BY date
			ROWS 6 PRECEDING
		) AS new_positives_reported_7d
	FROM {{ source('hhs', 'diagnostic_lab_testing_v3') }}
	GROUP BY "$path", date
)
SELECT
	file_datetime,
	date,
	new_results_reported_7d,
	new_positives_reported_7d,
	100.0 * new_positives_reported_7d / new_conclusives_reported_7d
		AS positivity
FROM raw
WHERE day_of_week(date) = 3
ORDER BY date DESC, file_datetime DESC;