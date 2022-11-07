--
-- Municipal positivity, Bayesian-weighted with archipelago-wide
-- positivity, based on number of tests.
--
-- I'm not sure if this is the best way to weight these because of
-- population differences between municipialities--maybe per-capita
-- is a better idea?  This might penalize smaller municipalities and
-- underserved ones.
--
WITH bulletins AS (
	SELECT max(bulletin_date) AS bulletin_date
	FROM {{ ref('municipal_tests_collected_agg') }}
), archipelago AS (
	SELECT
		bulletin_date,
		collected_date,
		test_type,
		count(DISTINCT municipality) municipalities,
		sum(sum(specimens)) OVER (
			PARTITION BY bulletin_date, test_type
			ORDER BY collected_date
			ROWS 6 PRECEDING
		) specimens_7d,
		sum(sum(positives)) OVER (
			PARTITION BY bulletin_date, test_type
			ORDER BY collected_date
			ROWS 6 PRECEDING
		) positives_7d
	FROM {{ ref('municipal_tests_collected_agg') }}
	INNER JOIN bulletins USING (bulletin_date)
	WHERE test_type = 'Antígeno'
	GROUP BY bulletin_date, collected_date, test_type
), municipalities AS (
	SELECT
		bulletin_date,
		municipality,
		collected_date,
		test_type,
		sum(specimens) OVER (
			PARTITION BY bulletin_date, municipality, test_type
			ORDER BY collected_date
			ROWS 6 PRECEDING
		) specimens_7d,
		sum(positives) OVER (
			PARTITION BY bulletin_date, municipality, test_type
			ORDER BY collected_date
			ROWS 6 PRECEDING
		) positives_7d
	FROM {{ ref('municipal_tests_collected_agg') }}
	INNER JOIN bulletins USING (bulletin_date)
	INNER JOIN {{ ref('municipal_abbreviations') }}
		USING (municipality)
	WHERE test_type = 'Antígeno'
)
SELECT
	bulletin_date,
	collected_date,
	municipality,
	test_type,
	municipalities.specimens_7d,
	100.0 * municipalities.positives_7d
		/ municipalities.specimens_7d
		AS raw_positivity,
	(100.0 * municipalities.positives_7d / municipalities.specimens_7d)
			* municipalities.specimens_7d
			/ (municipalities.specimens_7d + archipelago.specimens_7d / 78.0)
		+ (100.0 * archipelago.positives_7d / archipelago.specimens_7d)
			* (archipelago.specimens_7d / 78.0)
			/ (municipalities.specimens_7d + archipelago.specimens_7d / 78.0)
		AS bayesian_positivity
FROM municipalities
INNER JOIN archipelago USING (bulletin_date, collected_date, test_type)
ORDER BY bulletin_date DESC, collected_date DESC, test_type, bayesian_positivity DESC, municipality;
