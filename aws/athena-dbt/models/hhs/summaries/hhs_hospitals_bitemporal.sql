--
-- Bitemporal and cleaned-up view of HHS hospital utilization
-- timeseries dataset.
--
WITH maxes AS (
    SELECT max(file_timestamp) AS max_file_timestamp
    FROM  {{ ref('reported_hospital_utilization_timeseries') }}
    WHERE file_timestamp >= DATE '2021-08-14'
), downloads AS (
    SELECT
        file_timestamp,
        -- Edge case: cutover between two versions of the storage can
        -- lead (and has led) to two copies of the same file at different
        -- paths. So we pick the biggest version number on the path
        max(s3_path) AS s3_path
    FROM {{ ref('reported_hospital_utilization_timeseries') }}
    WHERE file_timestamp >= DATE '2021-08-14'
    GROUP BY file_timestamp
), grid AS (
    -- We want for each bulletin_date the earliest file that's at least
    -- one day later.  (Is this ideal? What if they issue a correction
    -- intra-day?)
    SELECT
        date(bulletin_date) bulletin_date,
        min(file_timestamp) AS file_timestamp
    FROM UNNEST(SEQUENCE(DATE '2021-08-14', DATE '{{ var("end_date") }}', INTERVAL '1' DAY))
    	AS dates(bulletin_date)
    INNER JOIN maxes
    	ON bulletin_date <= max_file_timestamp
	INNER JOIN downloads
	    ON bulletin_date < date(downloads.file_timestamp)
    GROUP BY bulletin_date
)
SELECT
    file_timestamp,
    bulletin_date,
    lead(bulletin_date)
        OVER bulletin
        AS valid_until,
    -- We subtract 1 from the date field because of the
    -- semantics of our `bulletin_date` field, which is
    -- "data as of the closing of this date."
	date(date_add('day', -1, date)) AS date,
	inpatient_beds,
	inpatient_beds_used,
	inpatient_beds_used_covid,
	inpatient_beds_used_covid_coverage,
	total_adult_patients_hospitalized_confirmed_and_suspected_covid
	    + total_pediatric_patients_hospitalized_confirmed_and_suspected_covid
	    AS total_patients_hospitalized_confirmed_and_suspected_covid,
    total_adult_patients_hospitalized_confirmed_covid
        + total_pediatric_patients_hospitalized_confirmed_covid
        AS total_patients_hospitalized_confirmed_covid,
	total_adult_patients_hospitalized_confirmed_and_suspected_covid,
	total_adult_patients_hospitalized_confirmed_covid,
    total_pediatric_patients_hospitalized_confirmed_and_suspected_covid,
    total_pediatric_patients_hospitalized_confirmed_covid,
	staffed_icu_adult_patients_confirmed_and_suspected_covid,

    -- We do two things to HHS's admissions data:
    --
    -- 1. Cut it off before mid-May 2021 because it is bad before this
    -- 2. Remove the `previous_day_*` names from columns because our
    --    semantics is that we already subtracted one day from the date.
	CASE WHEN date >= DATE '2021-05-16'
	THEN previous_day_admission_adult_covid_confirmed
		+ previous_day_admission_adult_covid_suspected
	END AS admission_adult_covid,
	CASE WHEN date >= DATE '2021-05-16'
	THEN (previous_day_admission_adult_covid_confirmed_coverage
		+ previous_day_admission_adult_covid_suspected_coverage) / 2.0
	END AS admission_adult_covid_coverage,

	CASE WHEN date >= DATE '2021-05-16'
	THEN previous_day_admission_adult_covid_confirmed
	END AS admission_adult_covid_confirmed,
	CASE WHEN date >= DATE '2021-05-16'
	THEN previous_day_admission_adult_covid_confirmed_coverage
	END AS admission_adult_covid_confirmed_coverage,

	CASE WHEN date >= DATE '2021-05-16'
	THEN previous_day_admission_pediatric_covid_confirmed
		+ previous_day_admission_pediatric_covid_suspected
	END AS admission_pediatric_covid,
	CASE WHEN date >= DATE '2021-05-16'
	THEN (previous_day_admission_pediatric_covid_confirmed_coverage
		+ previous_day_admission_pediatric_covid_suspected_coverage) / 2.0
	END AS admission_pediatric_covid_coverage,

	CASE WHEN date >= DATE '2021-05-16'
	THEN previous_day_admission_pediatric_covid_confirmed
	END AS admission_pediatric_covid_confirmed,
	CASE WHEN date >= DATE '2021-05-16'
	THEN previous_day_admission_pediatric_covid_confirmed_coverage
	END AS admission_pediatric_covid_confirmed_coverage,

	CASE WHEN date >= DATE '2021-05-16'
	THEN previous_day_admission_adult_covid_confirmed
        + previous_day_admission_pediatric_covid_confirmed
	END AS admission_covid_confirmed,
	CASE WHEN date >= DATE '2021-05-16'
	THEN (previous_day_admission_adult_covid_confirmed_coverage
        + previous_day_admission_pediatric_covid_confirmed_coverage) / 2.0
	END AS admission_covid_confirmed_coverage
FROM {{ ref('reported_hospital_utilization_timeseries') }}
INNER JOIN downloads USING (file_timestamp, s3_path)
INNER JOIN grid USING (file_timestamp)
-- This is the date when we start getting this timeseries daily instead of weekly
WHERE file_timestamp >= DATE '2021-08-14'
-- And dates earlier than this don't look like they're right
AND date >= DATE '2020-03-01'
WINDOW bulletin AS (
	PARTITION BY date
	ORDER BY bulletin_date
)
ORDER BY bulletin_date, date;