--
-- This is our more sophisticated "positive rate" analysis, which we
-- prefer to call the confirmed vs. rejected cases rate.  The key idea
-- is we don't count followup tests, i.e. test administered to patients
-- that had a positive result in the past three months.
--

SELECT
	bulletin_date,
	collected_date,
	initial_positive_molecular AS novels,
	rejections
FROM {{ ref('biostatistics_encounters_agg') }}
WHERE bulletin_date > DATE '2020-04-24'
ORDER BY bulletin_date, collected_date;
