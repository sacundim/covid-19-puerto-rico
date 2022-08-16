--
-- The CDC's Community Levels (a.k.a. the "green map") dataset.
--
SELECT
    {{ hhs_parse_filename_date('"$path"') }}
		AS file_timestamp,
	date_add('day', -1, date({{ hhs_parse_filename_date('"$path"') }}))
		AS bulletin_date,
	date_updated,
    county,
    {{ int_to_digits('county_fips', 5) }}
        AS county_fips,
    state,
    county_population,
    health_service_area_number,
    health_service_area,
    health_service_area_population,
    covid_inpatient_bed_utilization,
    covid_hospital_admissions_per_100k,
    covid_cases_per_100k,
    "covid-19_community_level" AS covid_19_community_level
FROM {{ source('hhs', 'community_levels_by_county_v3') }}

UNION ALL

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
    -- The type inference for V3 above gives us BIGINT, so BIGINT it is here...
    CAST(nullif(county_population, '') AS BIGINT)
    	AS county_population,
    CAST(nullif(health_service_area_number, '') AS BIGINT)
    	AS health_service_area_number,
    health_service_area,
    CAST(nullif(health_service_area_population, '') AS BIGINT)
    	AS health_service_area_population,
    CAST(nullif(covid_inpatient_bed_utilization, '') AS DOUBLE) / 100.0
    	AS covid_inpatient_bed_utilization,
    CAST(nullif(covid_hospital_admissions_per_100k, '') AS DOUBLE)
    	AS covid_hospital_admissions_per_100k,
    CAST(nullif(covid_cases_per_100k, '') AS DOUBLE)
    	AS covid_cases_per_100k,
    covid_19_community_level
FROM {{ source('hhs', 'community_levels_by_county_v2') }}

ORDER BY file_timestamp, date_updated, state, county_fips;