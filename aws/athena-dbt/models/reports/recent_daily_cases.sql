SELECT
    encounters.bulletin_date,
	encounters.collected_date AS datum_date,
	encounters.encounters AS tests,
    sum(encounters.encounters) OVER (
    	PARTITION BY encounters.bulletin_date
    	ORDER BY encounters.collected_date
    ) cumulative_tests,
	encounters.molecular AS pcr,
    sum(encounters.molecular) OVER (
    	PARTITION BY encounters.bulletin_date
    	ORDER BY encounters.collected_date
    ) cumulative_pcr,
	encounters.antigens AS antigens,
    sum(encounters.antigens) OVER (
    	PARTITION BY encounters.bulletin_date
    	ORDER BY encounters.collected_date
    ) cumulative_antigens,
	encounters.cases,
    sum(encounters.cases) OVER (
    	PARTITION BY encounters.bulletin_date
    	ORDER BY encounters.collected_date
    ) cumulative_cases,
	hosp.previous_day_admission_adult_covid_confirmed
		+ hosp.previous_day_admission_adult_covid_suspected
		+ hosp.previous_day_admission_pediatric_covid_confirmed
		+ hosp.previous_day_admission_pediatric_covid_suspected
		AS admissions,
	sum(hosp.previous_day_admission_adult_covid_confirmed
		+ hosp.previous_day_admission_adult_covid_suspected
		+ hosp.previous_day_admission_pediatric_covid_confirmed
		+ hosp.previous_day_admission_pediatric_covid_suspected) OVER (
    	PARTITION BY encounters.bulletin_date
    	ORDER BY encounters.collected_date
    ) AS cumulative_admissions,
    hosp.inpatient_beds_used_covid,
	bul.deaths AS deaths,
    sum(bul.deaths) OVER (
    	PARTITION BY encounters.bulletin_date
    	ORDER BY encounters.collected_date
    ) cumulative_deaths
FROM {{ ref('bioportal_encounters_agg') }} encounters
LEFT OUTER JOIN {{ ref('bulletin_cases') }} bul
	ON bul.bulletin_date = encounters.bulletin_date
	AND bul.datum_date = encounters.collected_date
LEFT OUTER JOIN {{ ref('hhs_hospitals') }} hosp
	ON encounters.collected_date = hosp.date
	AND hosp.date >= DATE '2020-07-28'
-- We want 42 days of data, so we fetch 56 because we need to
-- calculate a 14-day average 42 days ago:
WHERE encounters.collected_date >= date_add('day', -56, encounters.bulletin_date)
ORDER BY encounters.bulletin_date DESC, encounters.collected_date DESC;
