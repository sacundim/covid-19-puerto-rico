SELECT
    {{ hhs_parse_filename_date('"$path"') }}
		AS file_timestamp,
	date(date_parse("Data As Of", '%m/%d/%Y'))
	    AS data_as_of,
	date(date_parse("Start Date", '%m/%d/%Y'))
	    AS start_date,
	date(date_parse("End Date", '%m/%d/%Y'))
	    AS end_date,
	"Group" as aggregation,
    year,
    month,
    state,
    sex,
    "Age Group" AS age_group,
    "COVID-19 Deaths" AS covid_deaths,
    "Total Deaths" AS total_deaths,
    "Pneumonia Deaths" AS pneumonia_deaths,
    "Pneumonia and COVID-19 Deaths" AS pneumonia_and_covid_deaths,
    "Influenza Deaths" AS influenza_deaths,
    "Pneumonia, Influenza, or COVID-19 Deaths" AS pneumonia_influenza_or_covid_deaths,
    "Footnote" AS footnote
FROM {{ source('hhs', 'provisional_covid_19_deaths_by_sex_and_age_parquet_v3') }}
ORDER BY data_as_of, end_date, state;