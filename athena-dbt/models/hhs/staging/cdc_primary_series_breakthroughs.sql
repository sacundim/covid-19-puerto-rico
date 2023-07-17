WITH max_path AS (
    SELECT max("$path") max_path
    FROM {{ source('hhs', 'rates_of_covid_19_cases_or_deaths_by_age_group_and_vaccination_status_v3') }}
)
SELECT
    {{ hhs_parse_filename_date('"$path"') }}
		AS file_timestamp,
    CAST("MMWR week" AS VARCHAR) AS mmwr_week,
    {{ parse_mmwr_week('"MMWR week"') }}
		AS mmwr_week_start,
    outcome,
    "Age group" AS age_group,
    "Vaccine product" AS vaccine_product,
    "Vaccinated with outcome" AS vaccinated_with_outcome,
    "Fully vaccinated population" AS fully_vaccinated_population,
    "Unvaccinated with outcome" AS unvaccinated_with_outcome,
    "Unvaccinated population" AS unvaccinated_population,
    "Crude vax IR" AS crude_vax_ir,
    "Crude unvax IR" AS crude_unvax_ir,
    "Crude IRR" AS crude_irr,
    "Age adjusted vax IR" AS age_adjusted_vax_ir,
    "Age adjusted unvax IR" AS age_adjusted_unvax_ir,
    "Age adjusted IRR" AS age_adjusted_irr,
    "Continuity correction" AS continuity_correction
FROM {{ source('hhs', 'rates_of_covid_19_cases_or_deaths_by_age_group_and_vaccination_status_v3') }}
INNER JOIN max_path
    ON max_path = "$path"