--
-- Case curve by report date, instead of test sample date as I usually prefer.
-- This compares the case curve from official Puerto Rico Department of Health
-- data to the one I compute using Biostatistics.
--
WITH population AS (
	SELECT
		3193496 AS popest2019,
		3285874 AS census2020,
		-- For whatever indefensible reason, NYT uses this:
		3406672 AS popest2016
), official AS (
	SELECT
		bulletin_date,
		sum(delta_confirmed_cases) + sum(delta_probable_cases)
			AS delta_cases,
		sum(sum(delta_confirmed_cases) + sum(delta_probable_cases)) OVER (
			ORDER BY bulletin_date
			ROWS 6 PRECEDING
		) / 7.0 AS mean_delta_cases
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
	official.mean_delta_cases "Official data",
	1e5 * official.mean_delta_cases / popest2019 "Per 100k (2019 pop. estimate)",
	mine.mean_delta_cases "My case curve",
	1e5 * mine.mean_delta_cases / popest2019 "Per 100k",
	mine.mean_delta_first_infections "First cases",
	mine.mean_delta_possible_reinfections "Possible reinfections"
FROM official
INNER JOIN mine
	USING (bulletin_date)
CROSS JOIN population
ORDER BY bulletin_date DESC;
