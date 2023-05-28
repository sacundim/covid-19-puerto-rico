SELECT
	bulletin_date,
	ranges.tier,
	ranges.lo AS tier_order,
	COALESCE(sum(delta_specimens) FILTER (
		WHERE delta_specimens > 0
	), 0) AS count,
	COALESCE(sum(delta_positive_specimens) FILTER (
		WHERE delta_positive_specimens > 0
	), 0) AS positive
FROM {{ ref('biostatistics_specimens_collected_agg') }}
INNER JOIN (VALUES (0, 3, '0-3'),
                   (4, 7, '4-7'),
                   (8, 14, '8-14'),
                   (14, NULL, '> 14')) AS ranges (lo, hi, tier)
	ON ranges.lo <= collected_age
	AND collected_age <= COALESCE(ranges.hi, 2147483647)
WHERE test_type = 'Molecular'
AND bulletin_date >= DATE '2023-05-17'
GROUP BY bulletin_date, ranges.lo, ranges.hi, ranges.tier
ORDER BY bulletin_date, ranges.lo;
