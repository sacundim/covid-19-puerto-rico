WITH population AS (
 	SELECT
 	    3193694 AS popest2019,
 	    3285874 AS census2020
), cases AS (
	SELECT 
		bulletin_date AS date,
		sum(delta_confirmed_cases) 
			+ sum(delta_probable_cases)
			AS cases
	FROM {{ ref('bulletin_cases') }}
	GROUP BY bulletin_date 
), hosp AS (
	WITH dates AS (
		SELECT max(bulletin_date) AS bulletin_date
		FROM {{ ref('hhs_hospitals_bitemporal') }}
	)
	SELECT
		date,
		previous_day_admission_adult_covid 
			+ previous_day_admission_pediatric_covid 
			AS admissions,
		inpatient_beds_used_covid occupancy,
		inpatient_beds capacity
    FROM {{ ref('hhs_hospitals_bitemporal') }}
	INNER JOIN dates USING (bulletin_date)
), rates AS (
	SELECT 
		date,
		1e5 * sum(cases) OVER (
			ORDER BY date
			ROWS 6 PRECEDING
		) / popest2019 AS case_rate,
		1e5 * sum(admissions) OVER (
			ORDER BY date
			ROWS 6 PRECEDING
		) / popest2019 AS admission_rate,
		100.0 * sum(occupancy) OVER (
			ORDER BY date
			ROWS 6 PRECEDING
		) / sum(capacity) OVER (
			ORDER BY date
			ROWS 6 PRECEDING
		) AS occupancy_pct
	FROM cases
	INNER JOIN hosp
		USING (date)
	CROSS JOIN population
)
SELECT
	date "Date",
	case_rate "Case rate",
	admission_rate "Hosp. admission rate",
	occupancy_pct "Hospital occupation %",
	CASE WHEN case_rate < 200
	THEN
		CASE
			WHEN admission_rate >= 20 OR occupancy_pct >= 15
			THEN 'High'
			WHEN admission_rate >= 10 OR occupancy_pct >= 10
			THEN 'Medium'
			ELSE 'Low'
		END
	ELSE
		CASE
			WHEN admission_rate >= 10 OR occupancy_pct >= 10
			THEN 'High'
			ELSE 'Medium'
		END
	END AS "Level"
FROM rates
ORDER BY date DESC;