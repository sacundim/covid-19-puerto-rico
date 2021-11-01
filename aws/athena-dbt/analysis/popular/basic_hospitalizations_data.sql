--
-- Datos básicos de hospitalizaciones
--
SELECT
	file_timestamp AS "Datos",
	date AS "Fecha",
	inpatient_beds_used_covid AS "Camas ocupadas por COVID",
	previous_day_admission_adult_covid_confirmed
		+ previous_day_admission_adult_covid_suspected
		+ previous_day_admission_pediatric_covid_confirmed
		+ previous_day_admission_pediatric_covid_suspected
		AS "Admisiones por COVID",
	staffed_icu_adult_patients_confirmed_and_suspected_covid
		AS "Camas UCI ocupadas por COVID",
	staffed_adult_icu_bed_occupancy AS "Camas UCI ocupadas (cualquier causa)",
	total_staffed_adult_icu_beds AS "Total camas UCI"
FROM {{ ref('hhs_hospitals') }}
ORDER BY date DESC;
