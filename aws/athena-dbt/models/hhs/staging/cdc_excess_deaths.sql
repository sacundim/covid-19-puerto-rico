{{
    config(
        pre_hook=[
            "MSCK REPAIR TABLE {{ source('hhs', 'excess_deaths_associated_with_covid_19_v4').render_hive() }}"
        ]
}}
SELECT
    {{ hhs_parse_filename_date('"$path"') }}
		AS file_timestamp,
	date_add('day', -1, date({{ hhs_parse_filename_date('"$path"') }}))
		AS bulletin_date,
    -- If I don't do these renames, Athena's CREATE TABLE AS breaks.
    "Week Ending Date" AS week_ending_date,
    "State",
    "Observed Number" AS observed_number,
    "Upper Bound Threshold" AS upper_bound_threshold,
    "Exceeds Threshold" AS exceeds_threshold,
    "Average Expected Count" AS average_expected_count,
    "Excess Estimate" AS excess_estimate,
    "Total Excess Estimate" AS total_excess_estimate,
    "Percent Excess Estimate" AS percent_excess_estimate,
    "Year",
    "Type",
    "Outcome",
    "Suppress",
    "Note"
FROM {{ source('hhs', 'excess_deaths_associated_with_covid_19_v3') }}

UNION ALL

SELECT
    file_timestamp,
	date_add('day', -1, date(file_timestamp)
		AS bulletin_date,
    -- If I don't do these renames, Athena's CREATE TABLE AS breaks.
    "Week Ending Date" AS week_ending_date,
    "State",
    "Observed Number" AS observed_number,
    "Upper Bound Threshold" AS upper_bound_threshold,
    "Exceeds Threshold" AS exceeds_threshold,
    "Average Expected Count" AS average_expected_count,
    "Excess Estimate" AS excess_estimate,
    "Total Excess Estimate" AS total_excess_estimate,
    "Percent Excess Estimate" AS percent_excess_estimate,
    "Year",
    "Type",
    "Outcome",
    "Suppress",
    "Note"
FROM {{ source('hhs', 'excess_deaths_associated_with_covid_19_v4') }}