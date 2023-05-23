SELECT
    encounters.bulletin_date,
	encounters.collected_date AS datum_date,
	encounters.encounters AS tests,
    sum(encounters.encounters) OVER cumulative
        AS cumulative_tests,
	encounters.molecular AS pcr,
    sum(encounters.molecular) OVER cumulative
        AS cumulative_pcr,
	encounters.antigens AS antigens,
    sum(encounters.antigens) OVER cumulative
        AS cumulative_antigens,
	encounters.cases,
    sum(encounters.cases) OVER cumulative
        AS cumulative_cases,
	hosp.admission_covid
		AS admissions,
	sum(hosp.admission_covid) OVER cumulative
	    AS cumulative_admissions,
    hosp.camas_covid hospitalized_currently,
	bul.deaths AS deaths,
    sum(bul.deaths) OVER cumulative
        AS cumulative_deaths
FROM {{ ref('biostatistics_encounters_agg') }} encounters
LEFT OUTER JOIN {{ ref('bulletin_cases') }} bul
	ON bul.bulletin_date = encounters.bulletin_date
	AND bul.datum_date = encounters.collected_date
LEFT OUTER JOIN {{ ref('hospitalizations') }} hosp
	ON encounters.bulletin_date = hosp.bulletin_date
	AND encounters.collected_date = hosp.date
-- We want 42 days of data, so we fetch 56 because we need to
-- calculate a 14-day average 42 days ago:
WHERE encounters.collected_date >= date_add('day', -56, encounters.bulletin_date)
WINDOW cumulative AS (
    PARTITION BY encounters.bulletin_date
    ORDER BY encounters.collected_date
)
ORDER BY
    encounters.bulletin_date,
    encounters.collected_date;
