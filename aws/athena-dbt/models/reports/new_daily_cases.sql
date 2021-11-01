SELECT
    encounters.bulletin_date,
	encounters.collected_date AS datum_date,
    encounters.rejections,
	nullif(coalesce(bul.confirmed_cases, 0)
    	    + coalesce(bul.probable_cases, 0), 0)
	    AS official,
	encounters.cases AS bioportal,
	bul.deaths AS deaths,
	hosp.previous_day_admission_adult_covid_confirmed
		+ hosp.previous_day_admission_adult_covid_suspected
		+ hosp.previous_day_admission_pediatric_covid_confirmed
		+ hosp.previous_day_admission_pediatric_covid_suspected
		AS hospital_admissions
FROM {{ ref('bioportal_encounters_agg') }} encounters
LEFT OUTER JOIN {{ ref('bulletin_cases') }} bul
	ON bul.bulletin_date = encounters.bulletin_date
	AND bul.datum_date = encounters.collected_date
LEFT OUTER JOIN {{ ref('hhs_hospitals') }} hosp
	ON encounters.collected_date = hosp.date
	AND hosp.date >= DATE '2020-07-28'
ORDER BY encounters.bulletin_date DESC, encounters.collected_date DESC;
