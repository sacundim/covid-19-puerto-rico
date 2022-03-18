WITH max_path AS (
    SELECT max("$path") max_path
    FROM {{ source('hhs', 'rates_of_covid_19_cases_or_deaths_by_age_group_and_vaccination_status') }}
)
SELECT
    {{ hhs_parse_filename_date('"$path"') }}
		AS file_timestamp,
    mmwr_week,
	date_add('day', -1, date(parse_datetime(mmwr_week, 'xxxxww')))
		AS mmwr_week_start,
    outcome,
    age_group,
    vaccine_product,
    CAST(nullif(vaccinated_with_outcome, '') AS INT)
    	AS vaccinated_with_outcome,
    CAST(nullif(fully_vaccinated_population, '') AS DOUBLE) -- Yes, DOUBLE
    	AS fully_vaccinated_population,
    CAST(nullif(unvaccinated_with_outcome, '') AS INT)
    	AS unvaccinated_with_outcome,
    CAST(nullif(unvaccinated_population, '') AS DOUBLE)  -- Yes, DOUBLE
    	AS unvaccinated_population,
    CAST(nullif(crude_vax_ir, '') AS DOUBLE)
    	AS crude_vax_ir,
    CAST(nullif(crude_unvax_ir, '') AS DOUBLE)
    	AS crude_unvax_ir,
    CAST(nullif(crude_irr, '') AS DOUBLE)
    	AS crude_irr,
    CAST(nullif(age_adjusted_vax_ir, '') AS DOUBLE)
    	AS age_adjusted_vax_ir,
    CAST(nullif(age_adjusted_unvax_ir, '') AS DOUBLE)
    	AS age_adjusted_unvax_ir,
    CAST(nullif(age_adjusted_irr, '') AS DOUBLE)
    	AS age_adjusted_irr,
    CAST(nullif(continuity_correction, '') AS INT)
    	AS continuity_correction
FROM {{ source('hhs', 'rates_of_covid_19_cases_or_deaths_by_age_group_and_vaccination_status') }}
INNER JOIN max_path
    ON max_path = "$path"
ORDER BY file_timestamp, mmwr_week_start;