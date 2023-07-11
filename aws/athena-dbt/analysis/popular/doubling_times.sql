WITH bulletins AS (
	SELECT max(bulletin_date) AS max_bulletin_date
	FROM {{ ref('biostatistics_encounters_agg') }}
)
SELECT
	bulletin_date,
	collected_date,
	(cumulative_cases - lag(cumulative_cases, 7) OVER (
		PARTITION BY bulletin_date
		ORDER BY collected_date
	)) cases_7,
	7 / (
		log2(cumulative_cases - lag(cumulative_cases, 7) OVER (
			PARTITION BY bulletin_date
			ORDER BY collected_date
		)) - log2(lag(cumulative_cases, 7) OVER (
			PARTITION BY bulletin_date
			ORDER BY collected_date
		) - lag(cumulative_cases, 14) OVER (
			PARTITION BY bulletin_date
			ORDER BY collected_date
		))
	) doubling_days_7,
	(cumulative_cases - lag(cumulative_cases, 14) OVER (
		PARTITION BY bulletin_date
		ORDER BY collected_date
	)) cases_14,
	14 / (
		log2(cumulative_cases - lag(cumulative_cases, 14) OVER (
			PARTITION BY bulletin_date
			ORDER BY collected_date
		)) - log2(lag(cumulative_cases, 14) OVER (
			PARTITION BY bulletin_date
			ORDER BY collected_date
		) - lag(cumulative_cases, 28) OVER (
			PARTITION BY bulletin_date
			ORDER BY collected_date
		))
	) doubling_days_14
FROM {{ ref('biostatistics_encounters_agg') }} bio
INNER JOIN bulletins
	ON bulletins.max_bulletin_date = bio.bulletin_date
ORDER BY bulletin_date, collected_date DESC;
