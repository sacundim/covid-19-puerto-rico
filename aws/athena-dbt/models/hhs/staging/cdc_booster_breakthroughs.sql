WITH max_path AS (
    SELECT max("$path") max_path
    FROM {{ source('hhs', 'rates_of_covid_19_cases_or_deaths_by_age_group_and_vaccination_status_and_booster_dose') }}
)
SELECT
    {{ hhs_parse_filename_date('"$path"') }}
		AS file_timestamp,
    mmwr_week,
    {{ parse_mmwr_week('mmwr_week') }}
		AS mmwr_week_start,
    outcome,
    age_group,
    vaccine_product,
    {{ cast_string_column('boosted_with_outcome', 'INT') }},
    {{ cast_string_column('boosted_population', 'DOUBLE') }}, -- Yes, DOUBLE
    {{ cast_string_column('primary_series_only_with_outcome', 'INT') }},
    {{ cast_string_column('primary_series_only_population', 'DOUBLE') }}, -- Yes, DOUBLE
    {{ cast_string_column('unvaccinated_with_outcome', 'INT') }},
    {{ cast_string_column('unvaccinated_population', 'DOUBLE') }}, -- Yes, DOUBLE
    {{ cast_string_column('crude_booster_ir', 'DOUBLE') }},
    {{ cast_string_column('crude_primary_series_only_ir', 'DOUBLE') }},
    {{ cast_string_column('crude_unvax_ir', 'DOUBLE') }},
    {{ cast_string_column('crude_booster_irr', 'DOUBLE') }},
    {{ cast_string_column('crude_irr', 'DOUBLE') }},
    {{ cast_string_column('age_adj_booster_ir', 'DOUBLE') }},
    {{ cast_string_column('age_adj_vax_ir', 'DOUBLE') }},
    {{ cast_string_column('age_adj_unvax_ir', 'DOUBLE') }},
    {{ cast_string_column('age_adj_booster_irr', 'DOUBLE') }},
    {{ cast_string_column('age_adj_irr', 'DOUBLE') }},
    {{ cast_string_column('continuity_correction', 'INT') }}
FROM {{ source('hhs', 'rates_of_covid_19_cases_or_deaths_by_age_group_and_vaccination_status_and_booster_dose') }}
INNER JOIN max_path
    ON max_path = "$path"
ORDER BY file_timestamp, mmwr_week_start;