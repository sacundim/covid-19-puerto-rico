--
-- A case curve from Biostatistics data. This doesn't agree with the
-- official reports' cases curve for a few reasons:
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
FROM {{ ref('biostatistics_encounters_agg') }}
ORDER BY bulletin_date, collected_date;
