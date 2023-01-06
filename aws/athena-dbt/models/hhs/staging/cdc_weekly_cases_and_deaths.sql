--
-- The CDC's cases-and-deaths daily dataset, that was discontinued in
-- October 2022, replaced with a weekly dataset.
--
SELECT
    {{ hhs_parse_filename_date('"$path"') }}
		AS file_timestamp,
	date_add('day', -1, date({{ hhs_parse_filename_date('"$path"') }}))
		AS bulletin_date,
    *
FROM {{ source('hhs', 'weekly_united_states_covid_19_cases_and_deaths_by_state_v3') }}
ORDER BY file_timestamp, date_updated, state, start_date;