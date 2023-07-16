WITH max_path AS (
    SELECT max("$path") max_path
    FROM {{ source('hhs', 'rates_of_covid_19_cases_or_deaths_by_age_group_and_vaccination_status_and_booster_dose_v3') }}
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
    boosted_with_outcome,
    boosted_population,
    primary_series_only_with_outcome,
    primary_series_only_population,
    unvaccinated_with_outcome,
    unvaccinated_population,
    crude_booster_ir,
    crude_primary_series_only_ir,
    crude_unvax_ir,
    crude_booster_irr,
    crude_irr,
    age_adj_booster_ir,
    age_adj_vax_ir,
    age_adj_unvax_ir,
    age_adj_booster_irr,
    age_adj_irr,
    continuity_correction
FROM {{ source('hhs', 'rates_of_covid_19_cases_or_deaths_by_age_group_and_vaccination_status_and_booster_dose_v3') }}
INNER JOIN max_path
    ON max_path = "$path"