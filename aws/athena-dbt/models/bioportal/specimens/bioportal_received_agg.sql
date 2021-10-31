--
-- Same data as `bioportal_tritemporal_deltas`, but aggregated
-- to `received_date`.
--

SELECT
	test_type,
	bulletin_date,
	received_date,
	date_diff('day', received_date, bulletin_date)
		AS reported_age,
	sum(tests) AS tests,
	sum(sum(tests)) OVER (
        PARTITION BY test_type, bulletin_date
        ORDER BY received_date
    ) AS cumulative_tests,
	sum(delta_tests) AS delta_tests,
	sum(positive_tests) AS positive_tests,
	sum(sum(positive_tests)) OVER (
        PARTITION BY test_type, bulletin_date
        ORDER BY received_date
    ) AS cumulative_positives,
	sum(delta_positive_tests) AS delta_positive_tests
FROM {{ ref('bioportal_tritemporal_deltas') }}
GROUP BY test_type, bulletin_date, received_date;