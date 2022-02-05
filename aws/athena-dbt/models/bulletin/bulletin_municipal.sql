--
-- Consolidated view of the old PDF bulletins' municipal cases tables.
--
SELECT
	bulletin_date,
	municipality,
	COALESCE(confirmed_cases, 0) + COALESCE(probable_cases, 0)
		AS cumulative_cases,
	COALESCE(confirmed_cases, 0)
		+ COALESCE(probable_cases, 0)
		- COALESCE(lag(confirmed_cases, 1, 0) OVER (
			PARTITION BY municipality
			ORDER BY bulletin_date
		), 0)
		- COALESCE(lag(probable_cases, 1, 0) OVER (
			PARTITION BY municipality
			ORDER BY bulletin_date
		), 0) AS delta_cases
FROM covid_pr_sources.bulletin_municipal_molecular pcr
FULL OUTER JOIN covid_pr_sources.bulletin_municipal_antigens anti
	USING (bulletin_date, municipality)
ORDER BY bulletin_date, municipality;