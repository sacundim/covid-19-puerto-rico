--
-- Stitch together the CDC breakthrough data files into a single table.
-- These three datasets must be compared very carefully, because the
-- population with data for each is different from the others!  I.e.
-- the `primary` dataset reports on more jurisdictions than the `booster1`
-- dataset that in turn has more coverage than `booster2`.
--
SELECT
	mmwr_week,
	mmwr_week_start,
	outcome,
	age_group,
	vaccine_product,

	primary.vaccinated_with_outcome
	    AS primary_vaccinated_with_outcome,
	primary.fully_vaccinated_population
		AS primary_vaccinated_population,
	primary.unvaccinated_with_outcome
	    AS primary_unvaccinated_with_outcome,
	primary.unvaccinated_population
	    AS primary_unvaccinated_population,
    primary.vaccinated_with_outcome
        + primary.unvaccinated_with_outcome
        AS primary_with_outcome,
	primary.fully_vaccinated_population
	    + primary.unvaccinated_population
	    AS primary_population,

	booster1.boosted_with_outcome
	    AS booster1_boosted_with_outcome,
	booster1.boosted_population
	    AS booster1_boosted_population,
	booster1.primary_series_only_with_outcome
	    AS booster1_primary_series_only_with_outcome,
	booster1.primary_series_only_population
	    AS booster1_primary_series_only_population,
    booster1.unvaccinated_with_outcome
        AS booster1_unvaccinated_with_outcome,
    booster1.unvaccinated_population
        AS booster1_unvaccinated_population,
    booster1.boosted_with_outcome
	    + booster1.primary_series_only_with_outcome
	    + booster1.unvaccinated_with_outcome
	    AS booster1_with_outcome,
    booster1.boosted_population
	    + booster1.primary_series_only_population
	    + booster1.unvaccinated_population
	    AS booster1_population,

	booster2.one_boosted_with_outcome
	    AS booster2_one_boosted_with_outcome,
	booster2.one_booster_population
	    AS booster2_one_booster_population,
	booster2.two_boosted_with_outcome
	    AS booster2_two_boosted_with_outcome,
	booster2.two_booster_population
	    AS booster2_two_booster_population,
    booster2.vaccinated_with_outcome
        AS booster2_primary_series_only_with_outcome,
    booster2.fully_vaccinated_population
        AS booster2_primary_series_only_population,
    booster2.unvaccinated_with_outcome
        AS booster2_unvaccinated_with_outcome,
    booster2.unvaccinated_population
        AS booster2_unvaccinated_population,
    booster2.one_boosted_with_outcome
	    + booster2.two_boosted_with_outcome
	    + booster2.vaccinated_with_outcome
	    + booster2.unvaccinated_with_outcome
	    AS booster2_with_outcome,
    booster2.one_booster_population
	    + booster2.two_booster_population
	    + booster2.fully_vaccinated_population
	    + booster2.unvaccinated_population
	    AS booster2_population
FROM {{ ref("cdc_primary_series_breakthroughs") }} primary
LEFT OUTER JOIN {{ ref("cdc_booster_breakthroughs") }} booster1
	USING (mmwr_week, mmwr_week_start, outcome, age_group, vaccine_product)
LEFT OUTER JOIN {{ ref("cdc_second_booster_breakthroughs") }} booster2
	USING (mmwr_week, mmwr_week_start, outcome, age_group, vaccine_product)
-- FIXME: As of 2022-07-23, these datasets don't actually join up on the
-- `age_group` column.  The primary dataset drills down into ages 18-29,
-- 30-49, 65-79 and 80+, but the breakthrough datasets aggregate to 18-49
-- and 65+.  So I'm restricting to 'all_ages_adj' for now.
WHERE age_group = 'all_ages_adj'
ORDER BY mmwr_week_start;