--
-- Cleaned-up view of hospitalization data according to HHS.
--
SELECT
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
FROM {{ ref('hhs_hospitals_raw') }}
