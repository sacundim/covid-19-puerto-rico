--
-- Recently received test results.  Split into
-- "recent" and "late" tests by sample date.
--
SELECT
	test_type,
	bulletin_date,
	threshold.value late_threshold,
	sum(delta_tests) FILTER (WHERE collected_age <= threshold.value)
		AS recent_tests,
	sum(delta_positive_tests) FILTER (WHERE collected_age <= threshold.value)
		AS recent_positive_tests,
	sum(delta_tests) FILTER (WHERE collected_age > threshold.value)
		AS late_tests,
	sum(delta_positive_tests) FILTER (WHERE collected_age > threshold.value)
		AS late_positive_tests
FROM {{ ref('biostatistics_specimens_collected_agg') }}
CROSS JOIN (VALUES (5)) AS threshold (value)
WHERE test_type IN ('Molecular', 'Antígeno')
GROUP BY test_type, bulletin_date, threshold.value
ORDER BY bulletin_date DESC, test_type DESC;
