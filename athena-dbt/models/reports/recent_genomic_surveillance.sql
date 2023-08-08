WITH dim AS (
	SELECT *
	FROM {{ ref('lineages_of_interest') }}
	INNER JOIN {{ ref('pango_lineages') }}
		USING (lineage)
), max_dates AS (
	SELECT
		bulletin_date,
		max(week_starting) max_week_starting,
		date(max(week_starting) - INTERVAL '91' DAY)
			AS min_week_starting
	FROM {{ ref('vigilancia_cube') }}
	GROUP BY bulletin_date
)
SELECT
	c.bulletin_date,
	week_starting,
	date_add('day', 6, week_starting)
	  AS week_ending,
	dim.category,
	sum(count) AS count
FROM {{ ref('vigilancia_cube') }} c
LEFT OUTER JOIN dim
	ON starts_with(c.unaliased, dim.unaliased)
INNER JOIN max_dates md
	ON md.bulletin_date = c.bulletin_date
	AND min_week_starting < week_starting
	AND week_starting < max_week_starting
GROUP BY
	c.bulletin_date,
	week_starting,
	dim.category
ORDER BY
	c.bulletin_date,
	week_starting,
	count DESC,
	dim.category
