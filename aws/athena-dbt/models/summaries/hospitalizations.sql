--
-- COVID-19 hospitalization and ICU occupancy, using PRDoH data
-- for recent dates, backfilling missing older PRDoH data with
-- COVID Tracking Project.
--
-- PRDoH doesn't publish daily admissions figures, so we use HHS
-- for that
--
SELECT
	bulletin_date,
	fe_reporte date,
	COALESCE(camas_adultos_covid + camas_ped_covid, tracking.hospitalized_currently)
		AS hospitalized_currently,
	COALESCE(camas_icu_covid + camas_picu_covid, tracking.in_icu_currently)
		AS in_icu_currently,
    previous_day_admission_adult_covid,
    previous_day_admission_pediatric_covid
FROM {{ ref('hospitales_daily') }} prdoh
LEFT OUTER JOIN {{ source('hhs', 'covid_tracking_hospitalizations') }} tracking
	ON tracking."date" = prdoh.fe_reporte
LEFT OUTER JOIN {{ ref('hhs_hospitals') }} hhs
	ON hhs.date = prdoh.fe_reporte
	-- Older HHS data is quite bad
	AND hhs.date >= DATE '2020-07-28'
WHERE fe_reporte >= DATE '2020-04-18'
ORDER BY bulletin_date DESC, fe_reporte;
