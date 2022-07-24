--
-- Weekly percentage of breakthrough deaths
--
SELECT
	mmwr_week_start,
	100.0 * primary_vaccinated_with_outcome / primary_with_outcome
		AS breakthrough_pct,
	100.0 * booster1_primary_series_only_with_outcome / booster1_with_outcome
		AS primary_only_breakthrough_pct,
	100.0 * booster1_boosted_with_outcome / booster1_with_outcome
		AS boosted_breakthrough_pct,
	100.0 * booster2_one_boosted_with_outcome / booster2_with_outcome
		AS one_boosted_breakthrough_pct,
	100.0 * booster2_two_boosted_with_outcome / booster2_with_outcome
		AS two_boosted_breakthrough_pct
FROM {{ ref("cdc_primary_series_breakthroughs") }} breakthrough
WHERE outcome = 'death'
AND age_group = 'all_ages_adj'
AND vaccine_product = 'all_types'
ORDER BY mmwr_week_start DESC;