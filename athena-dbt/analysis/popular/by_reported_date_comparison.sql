--
-- Cases by reported date according to official data, vs.
-- cases by reported date accoring to my analysis.
--
WITH official AS (
	SELECT
		bulletin_date,
		sum(delta_confirmed_cases) + sum(delta_probable_cases)
			AS delta_cases,
		avg(sum(delta_confirmed_cases) + sum(delta_probable_cases)) OVER (
			ORDER BY bulletin_date
			ROWS 6 PRECEDING
		) AS mean_delta_cases
	FROM {{ ref('bulletin_cases') }}
	GROUP BY bulletin_date
), mine AS (
	SELECT
		bulletin_date,
		sum(delta_cases) FILTER (
			WHERE date_diff('day', collected_date, bulletin_date) < 14
		) AS delta_cases,
		avg(sum(delta_cases) FILTER (
			WHERE date_diff('day', collected_date, bulletin_date) < 14
		)) OVER (
			ORDER BY bulletin_date
			ROWS 6 PRECEDING
		) AS mean_delta_cases,
		sum(delta_first_infections) FILTER (
			WHERE date_diff('day', collected_date, bulletin_date) < 14
		) AS delta_first_infections,
		avg(sum(delta_first_infections) FILTER (
			WHERE date_diff('day', collected_date, bulletin_date) < 14
		)) OVER (
			ORDER BY bulletin_date
			ROWS 6 PRECEDING
		) AS mean_delta_first_infections,
		sum(delta_possible_reinfections) FILTER (
			WHERE date_diff('day', collected_date, bulletin_date) < 14
		) AS delta_cases,
		avg(sum(delta_possible_reinfections) FILTER (
			WHERE date_diff('day', collected_date, bulletin_date) < 14
		)) OVER (
			ORDER BY bulletin_date
			ROWS 6 PRECEDING
		) AS mean_delta_possible_reinfections
	FROM {{ ref('biostatistics_encounters_agg') }}
	GROUP BY bulletin_date
)
SELECT
	bulletin_date "Report date",
--	official.delta_cases "Oficial",
	official.mean_delta_cases "Official",
--	1e5 * official.mean_delta_cases / 3.411e6 "Por 100k (NYT)",
	100.0 * (official.mean_delta_cases / lag(official.mean_delta_cases, 14) OVER (
		ORDER BY bulletin_date
	) - 1) AS "14-day change",
--	mine.delta_cases "Mío",
	mine.mean_delta_cases "Mine",
--	1e5 * mine.mean_delta_cases / 3.284e6 "Por 100k (Censo 2020)",
	100.0 * (mine.mean_delta_cases / lag(mine.mean_delta_cases, 14) OVER (
		ORDER BY bulletin_date
	) - 1) AS "14-day change",
	mine.mean_delta_first_infections "First cases",
	mine.mean_delta_possible_reinfections "Possible reinfections"
FROM official
INNER JOIN mine
	USING (bulletin_date)
ORDER BY bulletin_date DESC;