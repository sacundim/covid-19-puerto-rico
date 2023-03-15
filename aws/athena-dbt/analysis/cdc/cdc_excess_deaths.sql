WITH bulletins AS (
	SELECT DISTINCT bulletin_date
	FROM {{ ref('cdc_excess_deaths') }}
	ORDER BY bulletin_date DESC
	LIMIT 4
), base AS (
	SELECT
		bulletin_date version,
		date_add('day', -6, min(week_ending_date)) since,
		max(week_ending_date) until,
		sum(observed_number) FILTER (
			WHERE outcome = 'All causes'
		) AS all_causes,
		sum(observed_number) FILTER (
			WHERE outcome = 'All causes, excluding COVID-19'
		) AS excluding_covid,
		sum(excess_estimate) FILTER (
			WHERE outcome = 'All causes'
		) AS all_cause_excess
	FROM {{ ref('cdc_excess_deaths') }}
	WHERE bulletin_date >= DATE '2023-02-28'
	AND state != 'United States'
	AND type = 'Predicted (weighted)'
	AND DATE '2020-01-01' <= date_add('day', -6, week_ending_date)
	AND week_ending_date <= DATE '2022-12-31'
	GROUP BY bulletin_date, type
)
SELECT
	*,
	-- This one gives us, indirectly, USA COVID-19 deaths by
	-- date of death instead of reported date:
	all_causes - excluding_covid
		AS covid
FROM base
ORDER BY version DESC;