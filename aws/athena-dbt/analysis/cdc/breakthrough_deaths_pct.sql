--
-- Weekly percentage of breakthrough deaths. Very roughly follows
-- the following Kaiser Family Foundation report, although I was
-- doing this sort of calculation before them, their is just very
-- well disseminated so it's good to roughly follow that.
--
-- https://www.healthsystemtracker.org/brief/covid19-and-other-leading-causes-of-death-in-the-us/
--
SELECT
	date_format(date_add('day', 6, mmwr_week_start), '%Y-%m') "Month",
	sum(primary_population)
		/ count(DISTINCT mmwr_week_start)
		AS "Population in primary dataset",
	100.0 * sum(primary_unvaccinated_with_outcome)
		/ sum(primary_with_outcome)
		AS "% no primary series",
	100.0 * sum(primary_vaccinated_with_outcome)
		/ sum(primary_with_outcome)
		AS "% at least primary",
	sum(booster1_population)
		/ count(DISTINCT mmwr_week_start)
		AS "Population in booster dataset",
	100.0 * sum(booster1_primary_series_only_with_outcome)
		/ sum(booster1_with_outcome)
		AS "% only primary",
	100.0 * sum(booster1_boosted_with_outcome)
		/ sum(booster1_with_outcome)
		AS "% at least one booster"
FROM {{ ref("cdc_breakthroughs") }} breakthrough
WHERE outcome = 'death'
AND vaccine_product = 'all_types'
-- We restrict to adults because that's what the widely-shared KFF chart
-- does.  All three datasets cover only populations eligible for the vaccine
-- dose that they concentrate on (primary, first booster, second booster)
-- so that seems most useful.
AND age_group != 'all_ages_adj' AND age_gte >= 18
GROUP BY date_format(date_add('day', 6, mmwr_week_start), '%Y-%m')
ORDER BY "Month" DESC;