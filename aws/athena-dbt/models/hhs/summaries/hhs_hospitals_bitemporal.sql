--
-- Bitemporal and cleaned-up view of HHS hospital utilization
-- timeseries dataset.
--
WITH maxes AS (
    SELECT max(file_timestamp) AS max_file_timestamp
    FROM  {{ ref('reported_hospital_utilization_timeseries') }}
    WHERE file_timestamp >= DATE '2021-08-14'
), downloads AS (
    SELECT DISTINCT file_timestamp
    FROM {{ ref('reported_hospital_utilization_timeseries') }}
    WHERE file_timestamp >= DATE '2021-08-14'
), grid AS (
    SELECT 
        date(bulletin_date) bulletin_date,
        max(file_timestamp) AS file_timestamp
    FROM UNNEST(SEQUENCE(DATE '2021-08-14', DATE '{{ var("end_date") }}', INTERVAL '1' DAY))
    	AS dates(bulletin_date)
	INNER JOIN downloads
	    ON bulletin_date <= downloads.file_timestamp
	    AND downloads.file_timestamp < date_add('day', 1, bulletin_date)
    INNER JOIN maxes
    	ON bulletin_date <= max_file_timestamp
    GROUP BY bulletin_date
)
SELECT
    file_timestamp,
    bulletin_date,
	date,
	inpatient_beds,
	inpatient_beds_used,
	inpatient_beds_used_covid,
	staffed_icu_adult_patients_confirmed_and_suspected_covid,
    -- HHS's admissions data is bad before this
	CASE WHEN date >= DATE '2021-05-16'
	THEN previous_day_admission_adult_covid_confirmed
		+ previous_day_admission_adult_covid_suspected
	END AS previous_day_admission_adult_covid,
	CASE WHEN date >= DATE '2021-05-16'
	THEN previous_day_admission_pediatric_covid_confirmed
		+ previous_day_admission_pediatric_covid_suspected
	END AS previous_day_admission_pediatric_covid
FROM {{ ref('reported_hospital_utilization_timeseries') }}
INNER JOIN grid USING (file_timestamp)
-- This is the date when we start getting this timeseries daily instead of weekly
WHERE file_timestamp >= DATE '2021-08-14'
-- And dates earlier than this don't look like they're right
AND date >= DATE '2020-03-01'
ORDER BY bulletin_date, date;