--
-- The CDC's Community Levels (a.k.a. the "green map") dataset.
--
SELECT
    {{ hhs_parse_filename_date('"$path"') }}
		AS file_timestamp,
	date_add('day', -1, date({{ hhs_parse_filename_date('"$path"') }}))
		AS bulletin_date,
	date(date_parse(date_updated, '%Y-%m-%d'))
		AS date_updated,
    county,
    county_fips,
    state,
    CAST(nullif(county_population, '') AS INT)
    	AS county_population,
    CAST(nullif(health_service_area_number, '') AS INT)
    	AS health_service_area_number,
    health_service_area,
    CAST(nullif(health_service_area_population, '') AS INT)
    	AS health_service_area_population,
    CAST(nullif(covid_inpatient_bed_utilization, '') AS DOUBLE) / 100.0
    	AS covid_inpatient_bed_utilization,
    CAST(nullif(covid_hospital_admissions_per_100k, '') AS DOUBLE)
    	AS covid_hospital_admissions_per_100k,
    CAST(nullif(covid_cases_per_100k, '') AS DOUBLE)
    	AS covid_cases_per_100k,
    "covid-19_community_level" AS covid_19_community_level
FROM {{ source('hhs', 'community_levels_by_county') }}
WHERE state = 'Puerto Rico'
ORDER BY file_timestamp, date_updated, county_fips;