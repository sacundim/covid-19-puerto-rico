----------------------------------------------------------------------------------
----------------------------------------------------------------------------------
--
-- Deaths according to Bioportal, which I don't think is as reliable
-- as the daily report.
--

WITH first_clean AS (
	SELECT
		date(downloaded_date) AS downloaded_date,
	    CAST(from_iso8601_timestamp(downloadedAt) AS TIMESTAMP)
	        AS downloaded_at,
	    CAST(from_iso8601_timestamp(downloadedAt) AS DATE) - INTERVAL '1' DAY
	        AS bulletin_date,
	    date(from_iso8601_timestamp(nullif(deathDate, '')) AT TIME ZONE 'America/Puerto_Rico')
	        AS raw_death_date,
	    date(from_iso8601_timestamp(nullif(reportDate, '')) AT TIME ZONE 'America/Puerto_Rico')
	        AS report_date,
	    region,
	    sex,
	   	ageRange AS age_range
	FROM covid_pr_sources.deaths_parquet_v1
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
