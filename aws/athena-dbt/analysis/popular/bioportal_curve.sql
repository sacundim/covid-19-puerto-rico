WITH bulletins AS (
	SELECT max(bulletin_date) AS bulletin_date
	FROM {{ ref('biostatistics_encounters_agg') }}
)
SELECT
	bulletin_date "Datos",
	collected_date AS "Muestras",
	encounters "Evaluados",
	sum(encounters) OVER (
	    PARTITION BY bulletin_date
	    ORDER BY collected_date
	    ROWS 6 PRECEDING
	) / 7.0 AS "Promedio",
	molecular "Molecular",
	sum(molecular) OVER (
	    PARTITION BY bulletin_date
	    ORDER BY collected_date
	    ROWS 6 PRECEDING
	) / 7.0 AS "Promedio",
	antigens "Antígeno",
	sum(antigens) OVER (
	    PARTITION BY bulletin_date
	    ORDER BY collected_date
	    ROWS 6 PRECEDING
	) / 7.0 AS "Promedio",
	cases "Casos",
	sum(cases) OVER (
	    PARTITION BY bulletin_date
	    ORDER BY collected_date
	    ROWS 6 PRECEDING
	) / 7.0 AS "Promedio"
FROM {{ ref('biostatistics_encounters_agg') }}
INNER JOIN bulletins
    USING (bulletin_date)
ORDER BY collected_date DESC;
