--
-- We call this the "naïve" positive rates chart because it uses the
-- simpler, more common metrics that don't account for followup test
-- load.
--

SELECT
	bioportal.test_type,
	bioportal.bulletin_date,
	collected_date,
	bioportal.tests,
	bioportal.positive_tests AS positives
FROM {{ ref('bioportal_collected_agg') }} bioportal
WHERE bioportal.test_type IN ('Molecular', 'Antígeno')
AND bioportal.bulletin_date > DATE '2020-04-24'
AND (
    -- Don't report on antigens earlier than Oct. 24 when
    -- it started in earnest.
	bioportal.test_type != 'Antígeno'
		OR bioportal.collected_date >= DATE '2020-10-24'
)
ORDER BY test_type, bulletin_date DESC, collected_date DESC;
