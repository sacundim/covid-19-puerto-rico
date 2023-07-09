--
-- Big municipality data table for a SPLOM chart.  Depends on
-- municipal bulletin data and minimal-info-unique-tests.
--
{{ config(enabled=false) }}
WITH tests AS (
    SELECT
        bulletin_date,
        municipality,
        sum(specimens) cumulative_specimens,
        sum(specimens) FILTER (
            WHERE test_type = 'Antígeno'
        ) AS cumulative_antigens,
        sum(positives) FILTER (
            WHERE test_type = 'Antígeno'
        ) AS cumulative_positive_antigens,
        sum(specimens) FILTER (
            WHERE test_type = 'Molecular'
        ) AS cumulative_molecular,
        sum(positives) FILTER (
            WHERE test_type = 'Molecular'
        ) AS cumulative_positive_molecular
    FROM {{ ref('biostatistics_specimens_municipal_agg') }}
    INNER JOIN {{ ref('municipal_abbreviations') }}
        USING (municipality)
    WHERE test_type IN ('Antígeno', 'Molecular')
    GROUP BY bulletin_date, municipality
), cases AS (
	SELECT
        bulletin_date,
        municipality,
        sum(new_cases) AS cumulative_cases
	FROM {{ ref('cases_municipal_agg') }}
	GROUP BY bulletin_date, municipality
)
SELECT
	local_date AS bulletin_date,
	municipio,
	race.fips,
	race.population,
	households_median,
	households_lt_10k_pct / 100.0
		AS households_lt_10k_pct,
    households_gte_200k_pct / 100.0
    	AS households_gte_200k_pct,
	white_alone,
	CAST(white_alone AS DOUBLE)
		/ race.population
		AS white_alone_pct,
	black_alone,
	CAST(black_alone AS DOUBLE)
		/ race.population
		AS black_alone_pct,
	cumulative_cases,
	1e3 * cumulative_cases / race.population
		AS cumulative_cases_1k,
	salud_total_dosis1 total_dosis1,
	CAST(salud_total_dosis1 AS DOUBLE)
		/ race.population
		AS total_dosis1_pct,
	salud_total_dosis2 total_dosis2,
	CAST(salud_total_dosis2 AS DOUBLE)
		/ race.population
		AS total_dosis2_pct,
	cumulative_specimens,
	1e3 * cumulative_specimens / race.population
		AS cumulative_specimens_1k,
	cumulative_antigens,
	1e3 * cumulative_antigens / race.population
		AS cumulative_antigens_1k,
	CAST(cumulative_positive_antigens AS DOUBLE)
		/ cumulative_antigens
		AS cumulative_antigen_positivity,
	cumulative_molecular,
	1e3 * cumulative_molecular / race.population
		AS cumulative_molecular_1k,
	CAST(cumulative_positive_molecular AS DOUBLE)
		/ cumulative_molecular
		AS cumulative_molecular_positivity
FROM tests
INNER JOIN {{ ref('municipal_vaccinations') }} vax
	ON vax.local_date = tests.bulletin_date
	AND vax.municipio = tests.municipality
INNER JOIN cases
	USING (bulletin_date, municipality)
INNER JOIN {{ ref('acs_2019_5y_race') }} race
	ON vax.municipio = race.municipality
INNER JOIN {{ ref('acs_2019_5y_household_income') }} income
	ON vax.municipio = income.municipality
ORDER BY bulletin_date, municipio;