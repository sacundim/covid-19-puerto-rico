--
-- We call this the "naïve" positive rates chart because it uses the
-- simpler, more common metrics that don't account for followup test
-- load.
--

SELECT
	biostatistics.test_type,
	biostatistics.bulletin_date,
	collected_date,
	biostatistics.specimens AS tests,
	biostatistics.positive_specimens AS positives
FROM {{ ref('biostatistics_specimens_collected_agg') }} biostatistics
WHERE biostatistics.test_type IN ('Molecular', 'Antígeno')
AND biostatistics.bulletin_date > DATE '2020-04-24'
AND (
    -- Don't report on antigens earlier than Oct. 24 when
    -- it started in earnest.
	biostatistics.test_type != 'Antígeno'
		OR biostatistics.collected_date >= DATE '2020-10-24'
)
ORDER BY test_type, bulletin_date, collected_date