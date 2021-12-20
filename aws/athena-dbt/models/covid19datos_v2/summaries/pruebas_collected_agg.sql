--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
--
-- Tests according to Salud.
--

WITH downloads AS (
    SELECT
        date_add('day', -1, date(downloaded_at AT TIME ZONE 'America/Puerto_Rico'))
            AS bulletin_date,
        max(downloaded_at) downloaded_at
    FROM {{ ref('casos') }}
    GROUP BY date_add('day', -1, date(downloaded_at AT TIME ZONE 'America/Puerto_Rico'))
)
SELECT
	bulletin_date,
	fe_prueba AS collected_date,
	count(*) tests,
	count(*) - lag(count(*), 1, 0) OVER (
		PARTITION BY fe_prueba
		ORDER BY downloaded_at
	) AS delta_tests,
	sum(count(*)) OVER (
		PARTITION BY downloaded_at
		ORDER BY fe_prueba
	) AS cumulative_tests,
	count(*) FILTER (
		WHERE co_tipo = 'MOLECULAR'
	) AS molecular,
	count(*) FILTER (
		WHERE co_tipo = 'MOLECULAR'
	) - lag(count(*) FILTER (
		WHERE co_tipo = 'MOLECULAR'
	), 1, 0) OVER (
		PARTITION BY fe_prueba
		ORDER BY downloaded_at
	) AS delta_molecular,
	sum(count(*) FILTER (
		WHERE co_tipo = 'MOLECULAR'
	)) OVER (
		PARTITION BY downloaded_at
		ORDER BY fe_prueba
	) AS cumulative_molecular,
	count(*) FILTER (
		WHERE co_tipo = 'MOLECULAR'
		AND co_resultado = 'POSITIVA'
	) AS positive_molecular,
	count(*) FILTER (
		WHERE co_tipo = 'MOLECULAR'
		AND co_resultado = 'POSITIVA'
	) - lag(count(*) FILTER (
		WHERE co_tipo = 'MOLECULAR'
		AND co_resultado = 'POSITIVA'
	), 1, 0) OVER (
		PARTITION BY fe_prueba
		ORDER BY downloaded_at
	) AS delta_positive_molecular,
	sum(count(*) FILTER (
		WHERE co_tipo = 'MOLECULAR'
		AND co_resultado = 'POSITIVA'
	)) OVER (
		PARTITION BY downloaded_at
		ORDER BY fe_prueba
	) AS cumulative_positive_molecular,
	count(*) FILTER (
		WHERE co_tipo = 'ANTIGENO'
	) AS antigens,
	count(*) FILTER (
		WHERE co_tipo = 'ANTIGENO'
	) - lag(count(*) FILTER (
		WHERE co_tipo = 'ANTIGENO'
	), 1, 0) OVER (
		PARTITION BY fe_prueba
		ORDER BY downloaded_at
	) AS delta_antigens,
	sum(count(*) FILTER (
		WHERE co_tipo = 'ANTIGENO'
	)) OVER (
		PARTITION BY downloaded_at
		ORDER BY fe_prueba
	) AS cumulative_antigens,
	count(*) FILTER (
		WHERE co_tipo = 'ANTIGENO'
		AND co_resultado = 'POSITIVA'
	) AS positive_antigens,
	count(*) FILTER (
		WHERE co_tipo = 'ANTIGENO'
		AND co_resultado = 'POSITIVA'
	) - lag(count(*) FILTER (
		WHERE co_tipo = 'ANTIGENO'
		AND co_resultado = 'POSITIVA'
	), 1, 0) OVER (
		PARTITION BY fe_prueba
		ORDER BY downloaded_at
	) AS delta_positive_antigens,
	sum(count(*) FILTER (
		WHERE co_tipo = 'ANTIGENO'
		AND co_resultado = 'POSITIVA'
	)) OVER (
		PARTITION BY downloaded_at
		ORDER BY fe_prueba
	) AS cumulative_positive_antigens
FROM {{ ref('pruebas') }} pruebas
INNER JOIN downloads
    USING (downloaded_at)
GROUP BY bulletin_date, downloaded_at, fe_prueba
ORDER BY bulletin_date, downloaded_at, fe_prueba;
