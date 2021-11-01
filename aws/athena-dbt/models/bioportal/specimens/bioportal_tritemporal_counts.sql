----------------------------------------------------------
----------------------------------------------------------
--
-- # Specimens analysis
--
-- These perform fairly straightforward aggregation of
-- Bioportal data, without deduplicating test specimens
-- taken during the same test encounter.  For a definition
-- of "specimen" vs. "encounter" see:
--
-- * https://covidtracking.com/analysis-updates/test-positivity-in-the-us-is-a-mess
--

--
-- Counts of tests from Bioportal, classified along four
-- time axes:
--
-- * `bulletin_date`, which is the data as-of date (that
--   allows us to "rewind" data to earlier state);
--
-- * `collected_date`, which is when test samples were taken
--
-- * `reported_date`, which is when the laboratory knew the
--   test result (but generally earlier than it communicated
--   it to PRDoH).
--
-- * `received_date`, which is when Bioportal says they received
--   the actual test result.
--
-- Yes, I know the name says "tritemporal" and now it's four
-- time axes.  Not gonna rename it right now.
--

WITH downloads AS (
	SELECT
		max(downloaded_at) max_downloaded_at,
		max(downloaded_date) max_downloaded_date
	FROM {{ ref('bioportal_orders_basic') }}
), bulletins AS (
	SELECT CAST(date_column AS DATE) AS bulletin_date
	FROM (
		VALUES (SEQUENCE(DATE '2020-04-24', DATE '2021-12-31', INTERVAL '1' DAY))
	) AS date_array (date_array)
	CROSS JOIN UNNEST(date_array) AS t2(date_column)
	INNER JOIN downloads
	    ON CAST(date_column AS DATE) < downloads.max_downloaded_date
)
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
GROUP BY test_type, bulletins.bulletin_date, collected_date, reported_date, received_date;
