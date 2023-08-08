WITH interest AS (
	SELECT
	  *,
	  row_number() OVER (
	    ORDER BY root, numbers
	  ) AS category_order
	FROM {{ ref('lineages_of_interest') }}
	INNER JOIN {{ ref('pango_lineages') }}
		USING (lineage)
), max_dates AS (
	SELECT
		bulletin_date,
		max(week_starting) max_week_starting,
		date(max(week_starting) - INTERVAL '168' DAY)
			AS min_week_starting
	FROM {{ ref('vigilancia_cube') }}
	GROUP BY bulletin_date
)
SELECT
	kube.bulletin_date,
	week_starting,
	date_add('day', 6, week_starting)
	  AS week_ending,
	parent.category,
	arbitrary(category_order) AS category_order,
	sum(count) AS count
FROM {{ ref('vigilancia_cube') }} kube
INNER JOIN {{ ref('pango_lineages') }} children
  ON kube.lineage = children.lineage
LEFT OUTER JOIN interest parent
  ON children.root = parent.root
  AND slice(children.numbers, 1, cardinality(parent.numbers)) = parent.numbers
INNER JOIN max_dates md
	ON md.bulletin_date = kube.bulletin_date
	AND min_week_starting < week_starting
	AND week_starting <= max_week_starting
WHERE NOT EXISTS (
  SELECT *
  FROM interest exclusions
  WHERE children.root = exclusions.root
  AND slice(children.numbers, 1, cardinality(exclusions.numbers)) = exclusions.numbers
  AND exclusions.root = parent.root
  AND slice(exclusions.numbers, 1, cardinality(parent.numbers)) = parent.numbers
  AND cardinality(exclusions.numbers) > cardinality(parent.numbers)
)
GROUP BY
	kube.bulletin_date,
	week_starting,
	parent.category
ORDER BY
	kube.bulletin_date,
	week_starting,
	category_order,
	count DESC
