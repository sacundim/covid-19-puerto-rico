WITH max_path AS (
    SELECT max("$path") max_path
    FROM {{ source('hhs', 'rates_of_covid_19_cases_or_deaths_by_age_group_and_vaccination_status_and_second_booster_dose_v3') }}
)
SELECT
    {{ hhs_parse_filename_date('"$path"') }}
		AS file_timestamp,
    CAST(mmwr_week AS VARCHAR)
        AS mmwr_week,
    {{ parse_mmwr_week('mmwr_week') }}
		AS mmwr_week_start,
    outcome,
    age_group,
    vaccine_product,
    one_boosted_with_outcome,
    one_booster_population,
    two_boosted_with_outcome,
    two_booster_population,
    vaccinated_with_outcome,
    fully_vaccinated_population,
    unvaccinated_with_outcome,
    unvaccinated_population,
    crude_one_booster_ir,
    crude_two_booster_ir,
    crude_vax_ir,
    crude_unvax_ir,
    crude_irr,
    crude_one_booster_irr,
    crude_two_booster_irr,
    crude_one_two_booster_irr,
    age_adj_one_booster_ir,
    age_adj_two_booster_ir,
    age_adj_vax_ir,
    age_adj_unvax_ir,
    age_adj_one_booster_irr,
    age_adj_two_booster_irr,
    age_adj_vax_irr,
    continuity_correction
FROM {{ source('hhs', 'rates_of_covid_19_cases_or_deaths_by_age_group_and_vaccination_status_and_second_booster_dose_v3') }}
INNER JOIN max_path
    ON max_path = "$path"