--
-- The CDC's cases-and-deaths daily dataset, that was discontinued in
-- October 2022, replaced with a weekly dataset.
--
SELECT
    {{ hhs_parse_filename_date('"$path"') }}
		AS file_timestamp,
	date_add('day', -1, date({{ hhs_parse_filename_date('"$path"') }}))
		AS bulletin_date,
	date_updated,
	state,
	start_date,
	end_date,
	tot_cases,
	new_cases,
	tot_deaths,
	new_deaths,
	new_historic_cases,
	new_historic_deaths
FROM {{ source('hhs', 'weekly_united_states_covid_19_cases_and_deaths_by_state_v2') }}

UNION ALL

SELECT
    {{ hhs_parse_filename_date('"$path"') }}
		AS file_timestamp,
	date_add('day', -1, date({{ hhs_parse_filename_date('"$path"') }}))
		AS bulletin_date,
	date(date_parse(date_updated, '%m/%d/%Y')) date_updated,
	state,
	date(date_parse(start_date, '%m/%d/%Y')) start_date,
	date(date_parse(end_date, '%m/%d/%Y')) end_date,
	tot_cases,
	new_cases,
	tot_deaths,
	new_deaths,
	new_historic_cases,
	new_historic_deaths
FROM {{ source('hhs', 'weekly_united_states_covid_19_cases_and_deaths_by_state_v3') }}

ORDER BY file_timestamp, date_updated, state, start_date;