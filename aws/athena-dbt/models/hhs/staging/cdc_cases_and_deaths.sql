SELECT
    {{ hhs_parse_filename_date('"$path"') }}
		AS file_timestamp,
	date_add('day', -1, date({{ hhs_parse_filename_date('"$path"') }}))
		AS bulletin_date,
	date(date_parse(submission_date, '%m/%d/%Y')) AS submission_date,
    state,
    tot_cases,
    conf_cases,
    prob_cases,
    new_case,
    pnew_case,
    tot_death,
    conf_death,
    prob_death,
    new_death,
    pnew_death,
    created_at,
    consent_cases,
    consent_deaths
FROM {{ source('hhs', 'united_states_covid_19_cases_and_deaths_by_state_v3') }}
ORDER BY file_timestamp, state, submission_date;