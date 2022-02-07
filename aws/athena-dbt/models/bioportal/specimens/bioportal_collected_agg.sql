--
-- Same data as `bioportal_tritemporal_deltas`, but aggregated
-- to `collected_date` (i.e., removes `reported_date` and
-- `received_date`).
--

SELECT
	test_type,
	bulletin_date,
	collected_date,
	date_diff('day', collected_date, bulletin_date)
		AS collected_age,
	sum(tests) AS tests,
	sum(sum(tests)) OVER (
        PARTITION BY test_type, bulletin_date
        ORDER BY collected_date
    ) AS cumulative_tests,
	sum(delta_tests) AS delta_tests,
	sum(positive_tests) AS positive_tests,
	sum(sum(positive_tests)) OVER (
        PARTITION BY test_type, bulletin_date
        ORDER BY collected_date
    ) AS cumulative_positives,
	sum(delta_positive_tests) AS delta_positive_tests
FROM {{ ref('bioportal_tritemporal_agg') }}
GROUP BY test_type, bulletin_date, collected_date;
