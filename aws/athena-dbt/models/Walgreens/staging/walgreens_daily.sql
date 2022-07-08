--
-- This crazy, crazy calculation reconstructs the daily counts from the 3-day sums.
--
-- If:
--
-- 1. We have a time series of 3-day sums `s1, s2, ...`;
-- 2. We notate the unknown daily 3-day sums as `d1, d2, ...`;
-- 3. We assume that unstated values before `s1` are zeroes;
--
-- ...then we can write the daily values d1, d2, ...` as a system of equations, e.g.:
--
--     d9 = s9 - d8 - d7
--     d8 = s8 - d7 - d6
--     d7 = s7 - d6 - d5
--     d6 = s6 - d5 - d4
--     d5 = s5 - d4 - d3
--     d4 = s4 - d3 - d2
--     d3 = s3 - d2 - d1
--     d2 = s2 - d1 - 0
--     d1 = s1 - 0 - 0
--
-- ...and we can substitute the daily values on the right hand side to get, for example:
--
--     d9 = s9 - s8 + s6 - s5 + s3 - s2
--
-- ...which we can rewrite as:
--
--     d9 = (s9 + s6 + s3) - (s8 + s5 + s2)
--
-- ...which can be computed with window functions if we partition by a modulo-3 row count,
-- do a running sum on date, and then subtract that from its lag.  Read the code, it's harder
-- to say this in words.
--
WITH modulos AS (
    SELECT
        downloaded_at,
        state,
        date,
        row_number() OVER (
            PARTITION BY downloaded_at, state
            ORDER BY date
        ) % 3 modulo,
        "3day_mvPreOmiBA11_nmrtr",
        "3day_mvPreOmiBA2_nmrtr",
        "3day_mvPreOther_nmrtr"
    FROM {{ ref('walgreens_dashboard_raw') }}
), skipping_sums AS (
	SELECT
        downloaded_at,
		state,
		date,
		modulo,
        "3day_mvPreOmiBA11_nmrtr",
        "3day_mvPreOmiBA2_nmrtr",
        "3day_mvPreOther_nmrtr",
		sum("3day_mvPreOmiBA11_nmrtr") OVER (
			PARTITION BY downloaded_at, state, modulo
			ORDER BY date
		) "3day_mvPreOmiBA11_nmrtr_modulo_sum",
		sum("3day_mvPreOmiBA2_nmrtr") OVER (
			PARTITION BY downloaded_at, state, modulo
			ORDER BY date
		) "3day_mvPreOmiBA2_nmrtr_modulo_sum",
		sum("3day_mvPreOther_nmrtr") OVER (
			PARTITION BY downloaded_at, state, modulo
			ORDER BY date
		) "3day_mvPreOther_nmrtr_modulo_sum"
	FROM modulos
), daily AS (
    SELECT
        downloaded_at,
        state,
        date,
        "3day_mvPreOmiBA11_nmrtr",
        "3day_mvPreOmiBA2_nmrtr",
        "3day_mvPreOther_nmrtr",
        "3day_mvPreOmiBA11_nmrtr_modulo_sum" - lag("3day_mvPreOmiBA11_nmrtr_modulo_sum", 1, 0) OVER (
            PARTITION BY downloaded_at, state
            ORDER BY date
        ) AS "PreOmiBA11",
        "3day_mvPreOmiBA2_nmrtr_modulo_sum" - lag("3day_mvPreOmiBA2_nmrtr_modulo_sum", 1, 0) OVER (
            PARTITION BY downloaded_at, state
            ORDER BY date
        ) AS "PreOmiBA2",
        "3day_mvPreOther_nmrtr_modulo_sum" - lag("3day_mvPreOther_nmrtr_modulo_sum", 1, 0) OVER (
            PARTITION BY downloaded_at, state
            ORDER BY date
        ) AS "PreOther"
    FROM skipping_sums
)
SELECT
    *,
    "PreOmiBA11"
		+ "PreOmiBA2"
		+ "PreOther"
		AS Total,
	sum("PreOmiBA11") OVER (
		PARTITION BY downloaded_at, state
		ORDER BY date
		ROWS 6 PRECEDING
	) / 7.0 AS PreOmiBA11_avg7,
	sum("PreOmiBA2") OVER (
		PARTITION BY downloaded_at, state
		ORDER BY date
		ROWS 6 PRECEDING
	) / 7.0 AS PreOmiBA2_avg7,
	sum("PreOther") OVER (
		PARTITION BY downloaded_at, state
		ORDER BY date
		ROWS 6 PRECEDING
	) / 7.0 AS PreOther_avg7,
	sum("PreOmiBA11" + "PreOmiBA2" + "PreOther") OVER (
		PARTITION BY downloaded_at, state
		ORDER BY date
		ROWS 6 PRECEDING
	) / 7.0 AS Total_avg7
FROM daily
ORDER BY
    downloaded_at,
    state,
    date;