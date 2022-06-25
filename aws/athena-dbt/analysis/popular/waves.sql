--
-- Una comparación cruda de las distintas olas en término de casos detectados.
-- Los puntos de comienzo y fin de cada una son a ojo, que no tiene tantísimo
-- de ciencia.
--
SELECT
	bulletin_date "Datos hasta",
	variant "Variante",
	since "Muestras desde",
	max(collected_date) "Hasta",
	sum(confirmed_cases) + sum(probable_cases)
		AS "Conteo oficial",
	sum(cases) AS "Mi análisis"
FROM {{ ref('bulletin_cases') }}
INNER JOIN {{ ref('bioportal_encounters_agg') }}
	USING (bulletin_date)
INNER JOIN (VALUES
    -- Esto es puro a ojo, especialmente en el 2020 que no
    -- es fácil discernir los fondos de las curvas:
	(DATE '2022-03-14', DATE '2023-01-01', 'Omicron BA.2/4/5'),
	(DATE '2021-12-12', DATE '2022-03-14', 'Omicron BA.1'),
	(DATE '2021-06-26', DATE '2021-12-12', 'Delta'),
	(DATE '2021-03-11', DATE '2021-06-26', 'Alfa'),
	(DATE '2020-10-08', DATE '2021-03-11', 'Ancestral'),
	(DATE '2020-05-27', DATE '2020-10-08', 'Ancestral'),
	(DATE '2020-03-01', DATE '2020-05-27', 'Ancestral')
) AS waves (since, until, variant)
	ON since <= datum_date
	AND datum_date < until
WHERE datum_date = collected_date
AND bulletin_date = (
	SELECT max(bulletin_date)
	FROM {{ ref('bioportal_encounters_agg') }}
)
GROUP BY bulletin_date, variant, since, until
ORDER BY since DESC;