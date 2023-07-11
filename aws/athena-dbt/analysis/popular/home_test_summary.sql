WITH encounters AS (
	SELECT
		casera.collected_date,
		casera.patient_id,
		min(lab.collected_date) min_lab_collected_date,
		max(lab.collected_date) max_lab_collected_date,
		bool_or(casera.positive) positive,
		count(*) FILTER (WHERE lab.patient_id IS NOT NULL) AS labs,
		bool_or(lab.positive) AS lab_result
	FROM {{ ref('biostatistics_tests') }} casera
	LEFT OUTER JOIN {{ ref('biostatistics_tests') }} lab
		ON lab.patient_id = casera.patient_id
		AND lab.test_type IN ('Molecular', 'Antígeno')
		AND casera.collected_date <= lab.collected_date
		AND lab.collected_date < date_add('day', 7, casera.collected_date)
	WHERE casera.test_type = 'Casera'
	AND casera.collected_date <= DATE '2022-06-15'
	GROUP BY casera.collected_date, casera.patient_id
)
SELECT
	max(collected_date) "Muestras caseras hasta",
	max(max_lab_collected_date) "Laboratorios hasta",
	count(DISTINCT patient_id) "Personas únicas",
	count(*) "Encuentros de prueba caseros",
	count(*) FILTER (
		WHERE positive
	) AS "Positivos caseros",
	count(*) FILTER (
		WHERE positive AND lab_result IS NOT NULL
	) AS "Positivos caseros con seguimiento en lab",
	count(*) FILTER (
		WHERE positive AND lab_result
	) "Positivos caseros con positivo en lab",
	count(*) FILTER (
		WHERE positive AND NOT lab_result
	) "Positivos caseros con negativo en lab"
FROM encounters;