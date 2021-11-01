--
-- COVID-19 hospitalization and ICU occupancy, using HHS data
-- for recent dates, backfilling bad older HHS data with
-- COVID Tracking Project.
--

WITH cutoff AS (
	SELECT DATE '2020-12-07' AS cutoff
)
SELECT
	date,
	hospitalized_currently,
	in_icu_currently
FROM covid_hhs_sources.covid_tracking_hospitalizations
INNER JOIN cutoff
	ON date < cutoff
UNION ALL
SELECT
	date,
	inpatient_beds_used_covid
		AS hospitalized_currently,
	staffed_icu_adult_patients_confirmed_and_suspected_covid
		AS in_icu_currently
FROM {{ ref('hhs_hospitals') }}
INNER JOIN cutoff
	-- Older HHS data is kinda messed up
	ON date >= cutoff
ORDER BY date DESC;
