----------------------------------------------------------
----------------------------------------------------------
--


WITH downloads AS (
	SELECT
		max(downloaded_at) max_downloaded_at,
		max(downloaded_date) max_downloaded_date
	FROM {{ ref('bioportal_orders_basic') }}
), bulletins AS (
	SELECT CAST(date_column AS DATE) AS bulletin_date
	FROM (
		VALUES (SEQUENCE(DATE '{{ var("first_bulletin_date") }}', DATE '{{ var("end_date") }}', INTERVAL '1' DAY))
	) AS date_array (date_array)
	CROSS JOIN UNNEST(date_array) AS t2(date_column)
	INNER JOIN downloads
	    ON CAST(date_column AS DATE) < downloads.max_downloaded_date
), counts AS (
    SELECT
        test_type,
        bulletins.bulletin_date,
        reported_date,
        collected_date,
        received_date,
        count(*) tests,
        count(*) FILTER (WHERE positive)
            AS positive_tests
    FROM {{ ref('bioportal_orders_basic') }} tests
    INNER JOIN downloads
        ON tests.downloaded_at = downloads.max_downloaded_at
    INNER JOIN bulletins
        ON bulletins.bulletin_date < downloads.max_downloaded_date
        AND tests.received_date <= bulletins.bulletin_date
    AND DATE '2020-03-01' <= collected_date
    AND collected_date <= received_date
    AND DATE '2020-03-01' <= reported_date
    AND reported_date <= received_date
    GROUP BY test_type, bulletins.bulletin_date, collected_date, reported_date, received_date
)
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
FROM counts
WHERE collected_date <= bulletin_date
AND reported_date <= bulletin_date;
