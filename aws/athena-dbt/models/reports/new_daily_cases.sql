SELECT
    encounters.bulletin_date,
	encounters.collected_date AS datum_date,
    encounters.rejections,
	nullif(coalesce(bul.confirmed_cases, 0)
    	    + coalesce(bul.probable_cases, 0), 0)
	    AS official,
	encounters.cases AS bioportal,
	bul.deaths AS deaths,
	hospitalizations.previous_day_admission_covid
		AS hospital_admissions,
    hospitalizations.camas_covid
        AS hospitalized_currently,
    hospitalizations.camas_total
        AS total_beds,
    hospitalizations.camas_icu_covid
        AS in_icu_currently
FROM {{ ref('bioportal_encounters_agg') }} encounters
LEFT OUTER JOIN {{ ref('bulletin_cases') }} bul
	ON bul.bulletin_date = encounters.bulletin_date
	AND bul.datum_date = encounters.collected_date
LEFT OUTER JOIN {{ ref('hospitalizations') }} hospitalizations
	ON encounters.bulletin_date = hospitalizations.bulletin_date
	AND encounters.collected_date = hospitalizations.date
ORDER BY encounters.bulletin_date DESC, encounters.collected_date DESC;
