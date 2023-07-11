--
-- Stitch together the CDC breakthrough data files into a single table.
-- These three datasets must be compared very carefully, because the
-- population with data for each is different from the others!  I.e.
-- the `primary` dataset reports on more jurisdictions than the `booster1`
-- dataset that in turn has more coverage than `booster2`.
--
SELECT
	primary.mmwr_week,
	primary.mmwr_week_start,
	primary.outcome,
	primary.vaccine_product,
	age_dim.boosted_age_group
	    AS age_group,
    min(age_gte) age_gte,
    max(age_lte) age_lte,

	sum(primary.vaccinated_with_outcome)
	    AS primary_vaccinated_with_outcome,
	sum(primary.fully_vaccinated_population)
		AS primary_vaccinated_population,
	sum(primary.unvaccinated_with_outcome)
	    AS primary_unvaccinated_with_outcome,
	sum(primary.unvaccinated_population)
	    AS primary_unvaccinated_population,
    sum(primary.vaccinated_with_outcome
        + primary.unvaccinated_with_outcome)
        AS primary_with_outcome,
	sum(primary.fully_vaccinated_population
	    + primary.unvaccinated_population)
	    AS primary_population,

    -- The `booster1` and `booster2` are actually guaranteed
    -- to have only one row per group from their source tables
    -- so we have to use `arbitrary` to not count double.
	arbitrary(booster1.boosted_with_outcome)
	    AS booster1_boosted_with_outcome,
	arbitrary(booster1.boosted_population)
	    AS booster1_boosted_population,
	arbitrary(booster1.primary_series_only_with_outcome)
	    AS booster1_primary_series_only_with_outcome,
	arbitrary(booster1.primary_series_only_population)
	    AS booster1_primary_series_only_population,
    arbitrary(booster1.unvaccinated_with_outcome)
        AS booster1_unvaccinated_with_outcome,
    arbitrary(booster1.unvaccinated_population)
        AS booster1_unvaccinated_population,
    arbitrary(booster1.boosted_with_outcome
	    + booster1.primary_series_only_with_outcome
	    + booster1.unvaccinated_with_outcome)
	    AS booster1_with_outcome,
    arbitrary(booster1.boosted_population
	    + booster1.primary_series_only_population
	    + booster1.unvaccinated_population)
	    AS booster1_population,

	arbitrary(booster2.one_boosted_with_outcome)
	    AS booster2_one_boosted_with_outcome,
	arbitrary(booster2.one_booster_population)
	    AS booster2_one_booster_population,
	arbitrary(booster2.two_boosted_with_outcome)
	    AS booster2_two_boosted_with_outcome,
	arbitrary(booster2.two_booster_population)
	    AS booster2_two_booster_population,
    arbitrary(booster2.vaccinated_with_outcome)
        AS booster2_primary_series_only_with_outcome,
    arbitrary(booster2.fully_vaccinated_population)
        AS booster2_primary_series_only_population,
    arbitrary(booster2.unvaccinated_with_outcome)
        AS booster2_unvaccinated_with_outcome,
    arbitrary(booster2.unvaccinated_population)
        AS booster2_unvaccinated_population,
    arbitrary(booster2.one_boosted_with_outcome
	    + booster2.two_boosted_with_outcome
	    + booster2.vaccinated_with_outcome
	    + booster2.unvaccinated_with_outcome)
	    AS booster2_with_outcome,
    arbitrary(booster2.one_booster_population
	    + booster2.two_booster_population
	    + booster2.fully_vaccinated_population
	    + booster2.unvaccinated_population)
	    AS booster2_population
FROM {{ ref("cdc_primary_series_breakthroughs") }} primary
-- These data sets have different age groups granularities, with the
-- primary one being more granular than the booster ones.  Here we
-- match them up and aggregate to the least common denominator.
INNER JOIN (VALUES ('5-11', 5, 11, '5-11'),
				   ('12-17', 12, 17, '12-17'),
				   ('18-29', 18, 29, '18-49'),
				   ('30-49', 30, 49, '18-49'),
				   ('50-64', 50, 64, '50-64'),
				   ('65-79', 65, 79, '65+'),
				   ('80+', 80, 200, '65+'),
				   ('all_ages_adj', 0, 200, 'all_ages_adj'))
				   AS age_dim (primary_age_group, age_gte, age_lte, boosted_age_group)
	ON primary.age_group = age_dim.primary_age_group
LEFT OUTER JOIN {{ ref("cdc_booster_breakthroughs") }} booster1
    ON booster1.mmwr_week = primary.mmwr_week
    AND booster1.mmwr_week_start = primary.mmwr_week_start
    AND booster1.outcome = primary.outcome
    AND booster1.vaccine_product = primary.vaccine_product
    AND booster1.age_group = age_dim.boosted_age_group
LEFT OUTER JOIN {{ ref("cdc_second_booster_breakthroughs") }} booster2
    ON booster2.mmwr_week = primary.mmwr_week
    AND booster2.mmwr_week_start = primary.mmwr_week_start
    AND booster2.outcome = primary.outcome
    AND booster2.vaccine_product = primary.vaccine_product
    AND booster2.age_group = age_dim.boosted_age_group
GROUP BY
	primary.mmwr_week,
	primary.mmwr_week_start,
	primary.outcome,
	primary.vaccine_product,
	age_dim.boosted_age_group