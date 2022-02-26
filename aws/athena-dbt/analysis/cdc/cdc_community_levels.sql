--
-- Draft implementation of of CDC Community Levels indicator
-- from Feburary 25, 2022
--
WITH population AS (
 	SELECT 3285874 AS pop2020
), cumulative AS (
	SELECT
		bulletin_date,
		datum_date,
		bioportal AS cases,
		sum(bioportal) OVER (
			PARTITION BY bulletin_date
			ORDER BY datum_date
		) AS cumulative_cases,
		hospital_admissions AS admissions,
		sum(hospital_admissions) OVER (
			PARTITION BY bulletin_date
			ORDER BY datum_date
		) AS cumulative_admissions,
		hospitalized_currently AS occupancy,
		sum(hospitalized_currently) OVER (
			PARTITION BY bulletin_date
			ORDER BY datum_date
		) AS cumulative_occupancy,
		total_beds AS capacity,
		sum(total_beds) OVER (
			PARTITION BY bulletin_date
			ORDER BY datum_date
		) AS cumulative_capacity
	FROM {{ ref('new_daily_cases') }}
), seven AS (
	SELECT
		bulletin_date,
		datum_date,
		cumulative_cases - lag(cumulative_cases, 7) OVER (
			PARTITION BY bulletin_date
			ORDER BY datum_date
		) cases,
		cumulative_admissions - lag(cumulative_admissions, 7) OVER (
			PARTITION BY bulletin_date
			ORDER BY datum_date
		) admissions,
		cumulative_occupancy - lag(cumulative_occupancy, 7) OVER (
			PARTITION BY bulletin_date
			ORDER BY datum_date
		) occupancy,
		cumulative_capacity - lag(cumulative_capacity, 7) OVER (
			PARTITION BY bulletin_date
			ORDER BY datum_date
		) capacity
	FROM cumulative
), per_capita AS (
	SELECT
		bulletin_date,
		datum_date,
		1e5 * cases / pop2020
			AS cases,
		1e5 * admissions / pop2020
			AS admissions,
		100.0 * CAST(occupancy AS DOUBLE)
			/ capacity
			AS pct_occupancy
	FROM seven
	CROSS JOIN population
)
SELECT
	bulletin_date "Data until",
	datum_date "Date",
	cases "Case rate",
	admissions "Hosp. admission rate",
	pct_occupancy "Hospital occupation %",
	CASE WHEN cases < 200
	THEN
		CASE
			WHEN admissions >= 20 OR pct_occupancy >= 15
			THEN 'High'
			WHEN admissions >= 10 OR pct_occupancy >= 10
			THEN 'Medium'
			ELSE 'Low'
		END
	ELSE
		CASE
			WHEN admissions >= 10 OR pct_occupancy >= 10
			THEN 'High'
			ELSE 'Medium'
		END
	END AS "Level"
FROM per_capita
WHERE bulletin_date = DATE '2022-02-24'
AND datum_date >= DATE '2021-12-07'
ORDER BY bulletin_date DESC, datum_date ASC;

