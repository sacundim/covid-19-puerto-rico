--
-- Una comparación cruda de las distintas olas en término de casos detectados.
-- Los puntos de comienzo y fin de cada una son a ojo, que no tiene tantísimo
-- de ciencia.
--
SELECT
	c.bulletin_date "Datos hasta",
	variant "Variante",
	date_diff('day', since, max(b.collected_date)) "Días",
	sum(c.confirmed_cases) + sum(c.probable_cases)
		AS "Casos (oficial)",
	sum(b.cases) AS "Casos (mío)",
	min(b.collected_date) "Desde",
	max(b.collected_date) "Hasta",
	sum(d.deaths) AS "Muertes",
	min(d.datum_date) AS "Desde",
	max(d.datum_date) AS "Hasta"
FROM {{ ref('bulletin_cases') }} c
LEFT OUTER JOIN {{ ref('bulletin_cases') }} d
	ON c.bulletin_date = d.bulletin_date
	-- Contamos muertes en periodos 14 días después de los casos
	AND d.datum_date = date_add('day', 14, c.datum_date)
INNER JOIN {{ ref('biostatistics_encounters_agg') }} b
	ON b.bulletin_date = c.bulletin_date
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
	ON since <= c.datum_date
	AND c.datum_date < until
WHERE c.datum_date = b.collected_date
AND b.bulletin_date = (
	SELECT max(bulletin_date)
	FROM {{ ref('biostatistics_encounters_agg') }}
)
GROUP BY c.bulletin_date, variant, since, until
ORDER BY since DESC;