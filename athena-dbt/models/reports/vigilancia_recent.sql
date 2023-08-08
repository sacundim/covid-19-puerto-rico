WITH dim AS (
	SELECT *
	FROM {{ ref('lineages_of_interest') }}
	INNER JOIN {{ ref('pango_lineages') }}
		USING (lineage)
)
SELECT
	c.bulletin_date,
	week_ending,
	dim.lineage,
	sum(count) AS count
FROM {{ ref('vigilancia_cube') }} c
LEFT OUTER JOIN dim
	ON starts_with(c.unaliased, dim.unaliased)
GROUP BY
	c.bulletin_date,
	week_ending,
	dim.lineage
ORDER BY
	c.bulletin_date,
	week_ending,
	count DESC,
	dim.lineage
