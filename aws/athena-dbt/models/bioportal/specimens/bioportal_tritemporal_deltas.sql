--
-- Same data as `bioportal_tritemporal_counts`, but enriched with
-- daily data changes (how many tests were reported from one
-- `bulletin_date` to the next).
--

SELECT
	test_type,
	bulletin_date,
	received_date,
	reported_date,
	collected_date,
	tests,
	tests - COALESCE(lag(tests) OVER (
        PARTITION BY test_type, collected_date, reported_date, received_date
	    ORDER BY bulletin_date
    ), 0) AS delta_tests,
	positive_tests,
	positive_tests - COALESCE(lag(positive_tests) OVER (
        PARTITION BY test_type, collected_date, reported_date, received_date
	    ORDER BY bulletin_date
    ), 0) AS delta_positive_tests
FROM {{ ref('bioportal_tritemporal_counts') }}
WHERE collected_date <= bulletin_date
AND reported_date <= bulletin_date;
