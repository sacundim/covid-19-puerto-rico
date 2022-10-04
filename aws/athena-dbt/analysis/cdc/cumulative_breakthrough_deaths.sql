WITH breakthroughs AS (
	-- Use the CDC breakthroughs dataset to compute what
	-- proportion of the deaths that it covers is among fully
	-- vaccinated people.  Note that CDC dataset doesn't cover
	-- all USA jurisdictions--when it started in mid-2021 it
	-- covered about 25 only, now (Oct. 2022) it's up to about 60.
	SELECT
		mmwr_week_start,
		date_add('day', 6, mmwr_week_start)
			AS week_ending_date,
		outcome,
		age_group,
		1.0 * primary_vaccinated_with_outcome
			/ (primary_vaccinated_with_outcome + primary_unvaccinated_with_outcome)
			AS proportion
	FROM {{ ref("cdc_breakthroughs") }}
	WHERE outcome = 'death'
	AND age_group = 'all_ages_adj'
	AND vaccine_product = 'all_types'
), deaths AS (
	-- Compute 7-day sums of deaths nationwide
	SELECT
		submission_date AS week_ending_date,
		sum(sum(new_death)) OVER (
			ORDER BY submission_date
			ROWS 6 PRECEDING
		) AS deaths
	FROM {{ ref('cdc_cases_and_deaths') }}
	WHERE bulletin_date = DATE '2022-10-02'
	GROUP BY submission_date
)
-- Here we join the breakthrough proportions calculation
-- from above with the nationwide deaths, multiply the
-- weekly nationwide deaths by the corresponding week's
-- breakthrough percentage, and compute a running sum.
SELECT
	week_ending_date
		AS "Week ending on",
	deaths AS "Reported deaths",
	100.0 * proportion
		AS "Estimated % breakthrough",
	deaths * proportion
		AS "Estimated breakthroughs",
	sum(deaths * proportion) OVER (
		ORDER BY week_ending_date
	) AS "Estimated cumulative breakthroughs"
FROM deaths
INNER JOIN breakthroughs USING (week_ending_date)
ORDER BY week_ending_date DESC;
