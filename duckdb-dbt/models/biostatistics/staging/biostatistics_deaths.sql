WITH first_clean AS (
	SELECT
		  CAST(downloaded_date AS DATE) AS downloaded_date,
      downloadedAt AS downloaded_at,
	    CAST(downloadedAt AT TIME ZONE 'America/Puerto_Rico' AS DATE)
	        - INTERVAL 1 DAY
	        AS bulletin_date,
	    CAST(deathId AS UUID) AS death_id,
      deathDate AS raw_death_date,
      deathReportDate AS raw_death_report_date,
	    nullif(sex, '') sex,
        {{ clean_age_range('ageRange') }} AS age_range,
        {{ clean_region('physicalRegion') }} AS region,
        nullif(vaccinationStatusAtDeath, '')
            AS vaccination_status_at_death
	FROM {{ source('biostatistics', 'deaths') }}
)
SELECT
	*,
	CASE
		WHEN raw_death_date < DATE '2020-01-01' AND month(raw_death_date) >= 3
		THEN CAST(strftime(raw_death_date, '2020-%m-%d') AS DATE)
		WHEN raw_death_date BETWEEN DATE '2020-01-01' AND DATE '2020-03-01'
		THEN raw_death_date + INTERVAL 1 YEAR
		ELSE raw_death_date
	END AS death_date,
	raw_death_report_date AS report_date
FROM first_clean