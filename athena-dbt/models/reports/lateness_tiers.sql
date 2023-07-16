--
-- For the LatenessTiers chart
--

SELECT
	bulletin_date,
	ranges.tier,
	ranges.lo AS tier_order,
	COALESCE(sum(delta_confirmed_cases) FILTER (
		WHERE delta_confirmed_cases > 0
	), 0) AS count
FROM {{ ref('bulletin_cases') }} ba
INNER JOIN (VALUES (0, 3, '0-3'),
				   (4, 7, '4-7'),
				   (8, 14, '8-14'),
				   (14, NULL, '> 14')) AS ranges (lo, hi, tier)
	ON ranges.lo <= age AND age <= COALESCE(ranges.hi, 2147483647)
WHERE bulletin_date > DATE '2020-04-24'
GROUP BY bulletin_date, ranges.lo, ranges.hi, ranges.tier
ORDER BY bulletin_date DESC, ranges.lo ASC;
