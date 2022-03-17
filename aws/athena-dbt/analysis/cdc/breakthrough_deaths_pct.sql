--
-- Weekly percentage of breakthrough deaths
--
SELECT
	mmwr_week_start,
	100.0 * vaccinated_with_outcome
		/ (vaccinated_with_outcome + unvaccinated_with_outcome)
		AS breakthrough_pct
FROM {{ ref("cdc_breakthroughs") }} breakthrough
WHERE outcome = 'death'
AND age_group = 'all_ages_adj'
AND vaccine_product = 'all_types'
ORDER BY mmwr_week_start DESC;