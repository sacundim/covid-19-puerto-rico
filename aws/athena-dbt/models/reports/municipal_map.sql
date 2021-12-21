--
-- For MunicipalMap
--

WITH deltas AS (
    -- We use a hybrid report-and-sample date grouping here, that the
    -- Puerto Rico Department of Health used for a very long time, which
    -- works as follows. For each date, we include cases that:
    --
    -- 1. Were initially reported on that date (`bulletin_date` column);
    -- 2. But whose `sample_date` falls in the 14-day period before that
    --    `bulletin_date`.
    --
    -- What this does, in effect, is exclude late-arriving data.
    --
    SELECT
        bulletin_date,
        municipality,
        pop2020,
        sum(delta_cases) FILTER (
            WHERE date_add('day', -14, bulletin_date) <= sample_date
        ) recent_delta_cases
    FROM {{ ref('cases_municipal_agg') }}
    GROUP BY bulletin_date, municipality, pop2020
)
-- The main query basically takes a 7-day and a 14-day sum of the above
SELECT
	cur.bulletin_date,
	cur.municipality,
	cur.pop2020,
	sum(all.recent_delta_cases) FILTER (
		WHERE date_add('day', -7, cur.bulletin_date) < all.bulletin_date
	) new_7day_cases,
	sum(all.recent_delta_cases) FILTER (
		WHERE date_add('day', -14, cur.bulletin_date) <= all.bulletin_date
		AND all.bulletin_date < date_add('day', -7, cur.bulletin_date)
	) previous_7day_cases
FROM deltas cur
INNER JOIN deltas all
    ON cur.municipality = all.municipality
    AND date_add('day', -14, cur.bulletin_date) <= all.bulletin_date
    AND all.bulletin_date <= cur.bulletin_date
GROUP BY
	cur.bulletin_date,
	cur.municipality,
	cur.pop2020
ORDER BY
	cur.bulletin_date,
	cur.municipality;
