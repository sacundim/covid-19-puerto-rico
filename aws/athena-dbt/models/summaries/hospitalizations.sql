--
-- COVID-19 hospitalization and ICU occupancy, using PRDoH data
-- for recent dates, backfilling missing older PRDoH data with
-- COVID Tracking Project.
--
-- PRDoH doesn't publish daily admissions figures, so we use HHS
-- for that
--
SELECT
	prdoh.bulletin_date,
	fe_reporte date,
	-- PrDoh fields. Convention: names in Spanish
	camas_adultos_total + camas_ped_total
	    AS camas_total,
	COALESCE(camas_adultos_covid + camas_ped_covid, tracking.hospitalized_currently)
		AS camas_covid,
	COALESCE(camas_icu_covid + camas_picu_covid, tracking.in_icu_currently)
		AS camas_icu_covid,
	-- HHS fields. Convention: names in English
    previous_day_admission_adult_covid
        + previous_day_admission_pediatric_covid
        AS previous_day_admission_covid,
    inpatient_beds,
    inpatient_beds_used_covid
FROM {{ ref('hospitales_daily') }} prdoh
LEFT OUTER JOIN {{ ref('covid_tracking_hospitalizations') }} tracking
	ON tracking."date" = prdoh.fe_reporte
/* One day:
LEFT OUTER JOIN {{ ref('hhs_hospitals_bitemporal') }} hhs
	ON hhs.bulletin_date = prdoh.bulletin_date
	AND hhs.date = prdoh.fe_reporte
*/
LEFT OUTER JOIN {{ ref('hhs_hospitals') }} hhs
	ON hhs.date = prdoh.fe_reporte
	-- Older HHS data is quite bad
	AND hhs.date >= DATE '2020-07-28'
WHERE fe_reporte >= DATE '2020-04-18'
ORDER BY bulletin_date DESC, fe_reporte;
