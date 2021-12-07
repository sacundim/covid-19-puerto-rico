--
-- Vaccination reporting lag
--
WITH agg AS (
	SELECT
		bulletin_date,
		dose_date,
		sum(delta_doses) FILTER (
			WHERE delta_doses >= 0
		) AS delta_doses,
		sum(delta_doses * date_diff('day', dose_date, bulletin_date)) FILTER (
			WHERE delta_doses >= 0
		) AS delta_doses_days
	FROM {{ ref('vacunacion_cube') }} vax
	WHERE dose_date >= date_add('day', -60, bulletin_date)
	GROUP BY bulletin_date, dose_date, dose_number
)
SELECT
	bulletin_date "Fecha de reporte",
	sum(delta_doses) "Dosis reportadas",
	1.0 * sum(delta_doses_days) / sum(delta_doses) "Rezago promedio (días)"
FROM agg
GROUP BY bulletin_date
ORDER BY bulletin_date DESC;

