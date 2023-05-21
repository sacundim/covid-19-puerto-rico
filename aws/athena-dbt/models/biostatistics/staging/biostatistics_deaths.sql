{{
    config(pre_hook=[
        "MSCK REPAIR TABLE {{ source('biostatistics', 'deaths_v2').render_hive() }}"
    ])
}}

WITH first_clean AS (
	SELECT
		date(downloaded_date) AS downloaded_date,
        downloadedAt AS downloaded_at,
	    CAST(downloadedAt AT TIME ZONE 'America/Puerto_Rico' AS DATE)
	        - INTERVAL '1' DAY
	        AS bulletin_date,
        deathId AS death_id,
        deathDate AS raw_death_date,
        deathReportDate AS raw_death_report_date,
	    nullif(sex, '') sex,
        {{ clean_age_range('ageRange') }} AS age_range,
        {{ clean_region('physicalRegion') }} AS region,
        nullif(vaccinationStatusAtDeath, '')
            AS vaccination_status_at_death
	FROM {{ source('biostatistics', 'deaths_v2') }}
)
SELECT
	*,
	CASE
		WHEN raw_death_date < DATE '2020-01-01' AND month(raw_death_date) >= 3
		THEN from_iso8601_date(date_format(raw_death_date, '2020-%m-%d'))
		WHEN raw_death_date BETWEEN DATE '2020-01-01' AND DATE '2020-03-01'
		THEN date_add('year', 1, raw_death_date)
		ELSE raw_death_date
	END AS death_date,
	raw_death_report_date AS report_date
FROM first_clean
ORDER BY bulletin_date, death_date;
