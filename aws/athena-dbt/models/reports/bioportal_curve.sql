--
-- A case curve from Bioportal data. This doesn't agree with the
-- official reports' cases curve for a few reasons:
--
-- 1. The deduplication in Bioportal's `patientId` field doesn't
--    work the same as the official bulletin, and in fact gives
--    very different results;
-- 2. Bioportal has fresher data than the official bulletin,
--    routinely by 2-3 days;
-- 3. This curve strives to use all data that Bioportal provides,
--    not just molecular test results; we will definitely count
--    antigen positives toward cases.
--

SELECT
	bulletin_date,
	collected_date,
	cases,
    sum(cases) OVER (
		PARTITION BY bulletin_date
		ORDER BY collected_date
	) AS cumulative_cases,
	cases - coalesce(lag(cases) OVER (
		PARTITION BY collected_date
		ORDER BY bulletin_date
	), 0) AS delta_cases
FROM {{ ref('bioportal_encounters_agg') }}
ORDER BY bulletin_date DESC, collected_date DESC;
