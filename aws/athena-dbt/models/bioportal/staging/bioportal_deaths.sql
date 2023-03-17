----------------------------------------------------------------------------------
----------------------------------------------------------------------------------
--
-- Deaths according to Bioportal, which I don't think is as reliable
-- as the daily report.
--

{{
    config(pre_hook=[
        "MSCK REPAIR TABLE {{ source('bioportal', 'deaths_v1').render_hive() }}",
        "MSCK REPAIR TABLE {{ source('bioportal', 'deaths_v5').render_hive() }}"
    ])
}}

WITH first_clean AS (
	SELECT
		date(downloaded_date) AS downloaded_date,
        {{ parse_filename_timestamp('"$path"') }}
            AS downloaded_at,
	    CAST({{ parse_filename_timestamp('"$path"') }} AT TIME ZONE 'America/Puerto_Rico' AS DATE)
	        - INTERVAL '1' DAY
	        AS bulletin_date,
	    date(from_iso8601_timestamp(nullif(deathDate, '')) AT TIME ZONE 'America/Puerto_Rico')
	        AS raw_death_date,
	    date(from_iso8601_timestamp(nullif(reportDate, '')) AT TIME ZONE 'America/Puerto_Rico')
	        AS report_date,
        {{ clean_region('region') }} AS region,
	    nullif(sex, '') sex,
        {{ clean_age_range('ageRange') }} AS age_range
	FROM {{ source('bioportal', 'deaths_v1') }}

	UNION ALL

	SELECT
		date(downloaded_date) AS downloaded_date,
        {{ parse_filename_timestamp('"$path"') }}
            AS downloaded_at,
	    CAST({{ parse_filename_timestamp('"$path"') }} AT TIME ZONE 'America/Puerto_Rico' AS DATE)
	        - INTERVAL '1' DAY
	        AS bulletin_date,
	    date(from_iso8601_timestamp(nullif(deathDate, '')) AT TIME ZONE 'America/Puerto_Rico')
	        AS raw_death_date,
	    date(from_iso8601_timestamp(nullif(reportDate, '')) AT TIME ZONE 'America/Puerto_Rico')
	        AS report_date,
        {{ clean_region('region') }} AS region,
	    nullif(sex, '') sex,
        {{ clean_age_range('ageRange') }} AS age_range
	FROM {{ source('bioportal', 'deaths_v5') }}
)
SELECT
	*,
	CASE
		WHEN raw_death_date < DATE '2020-01-01' AND month(raw_death_date) >= 3
		THEN from_iso8601_date(date_format(raw_death_date, '2020-%m-%d'))
		WHEN raw_death_date BETWEEN DATE '2020-01-01' AND DATE '2020-03-01'
		THEN date_add('year', 1, raw_death_date)
		ELSE raw_death_date
	END AS death_date
FROM first_clean
ORDER BY bulletin_date, death_date, report_date;
