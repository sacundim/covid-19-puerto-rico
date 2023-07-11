WITH bulletins AS (
	SELECT max(bulletin_date) AS bulletin_date
	FROM {{ ref("biostatistics_specimens_collected_agg") }}
), bitemporal AS (
	SELECT
		bulletin_date,
		collected_date,
		100.0 * sum(positive_tests) OVER (
		    PARTITION BY bulletin_date
		    ORDER BY collected_date
		    ROWS 6 PRECEDING
		) / sum(tests) OVER (
		    PARTITION BY bulletin_date
		    ORDER BY collected_date
		    ROWS 6 PRECEDING
		) AS positive_rate
	FROM {{ ref("biostatistics_specimens_collected_agg") }}
	WHERE test_type = 'Molecular'
)
SELECT
	perspectival.bulletin_date AS "Fecha de muestras",
	perspectival.positive_rate "Con datos solo hasta esa fecha",
	collected.bulletin_date AS "Datos más recientes",
	collected.positive_rate AS "Con datos más recientes",
	perspectival.positive_rate - collected.positive_rate AS "Inflación"
FROM bitemporal collected
INNER JOIN bulletins
	ON bulletins.bulletin_date = collected.bulletin_date
INNER JOIN bitemporal perspectival
	ON perspectival.bulletin_date = collected.collected_date
	AND perspectival.collected_date = perspectival.bulletin_date
ORDER BY "Fecha de muestras" DESC;