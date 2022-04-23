WITH cases AS (
	SELECT
		bulletin_date AS date,
		municipality,
		popest2019,
		1e5 * sum(sum(delta_cases)) OVER (
    		PARTITION BY municipality
			ORDER BY bulletin_date
			ROWS 6 PRECEDING
		) / popest2019 AS case_rate
	FROM {{ ref('cases_municipal_agg') }}
	GROUP BY bulletin_date, municipality, popest2019
), hosp AS (
	WITH dates AS (
		SELECT max(bulletin_date) AS bulletin_date
		FROM {{ ref('hhs_hospitals_bitemporal') }}
	), population AS (
	 	SELECT
	 	    3193694 AS popest2019,
	 	    3285874 AS census2020
	)
	SELECT
		date,
		1e5 * sum(previous_day_admission_adult_covid
			+ previous_day_admission_pediatric_covid) OVER (
			ORDER BY date
			ROWS 6 PRECEDING
		) / popest2019 AS admission_rate,
		100.0 * sum(inpatient_beds_used_covid) OVER (
			ORDER BY date
			ROWS 6 PRECEDING
		) / sum(inpatient_beds) OVER (
			ORDER BY date
			ROWS 6 PRECEDING
		) AS occupancy_pct
	FROM {{ ref('hhs_hospitals_bitemporal') }}
	INNER JOIN dates USING (bulletin_date)
	CROSS JOIN population
)
SELECT
	date "Date",
	municipality "Municipality",
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
FROM cases INNER JOIN hosp USING (date)
ORDER BY date DESC, municipality;