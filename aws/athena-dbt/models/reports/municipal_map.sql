--
-- For MunicipalMap
--

SELECT
	bulletin_date,
	municipality,
	pop2020,
	sum(new_cases) FILTER (
		WHERE date_add('day', -7, bulletin_date) <= sample_date
	) new_7day_cases,
	sum(new_cases) FILTER (
		WHERE date_add('day', -14, bulletin_date) <= sample_date
		AND sample_date < date_add('day', -7, bulletin_date)
	) previous_7day_cases,
	sum(new_cases) FILTER (
		WHERE date_add('day', -14, bulletin_date) <= sample_date
	) new_14day_cases,
	sum(new_cases) FILTER (
		WHERE sample_date < date_add('day', -14, bulletin_date)
	) previous_14day_cases
FROM {{ ref('cases_municipal_agg') }}
WHERE sample_date >= date_add('day', -28, bulletin_date)
GROUP BY
	bulletin_date,
	municipality,
	pop2020
ORDER BY
	bulletin_date,
	municipality;
