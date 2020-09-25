CREATE UNLOGGED TABLE bioportal_tests (
    id SERIAL,
    downloaded_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    bulletin_date DATE NOT NULL
        GENERATED ALWAYS AS (
            date(downloaded_at - INTERVAL '4 hour') - 1
        ) STORED,

    raw_collected_date DATE,
    raw_reported_date DATE,
    created_at TIMESTAMP,

    collected_date DATE NOT NULL
        GENERATED ALWAYS AS (
            CASE
                WHEN raw_collected_date >= '2020-01-01'
                THEN raw_collected_date
                WHEN raw_reported_date >= '2020-03-13'
                -- Suggested by @rafalab. He uses two days as the value and says
                -- that's the average, but my spot check says 2.8 days.
                THEN raw_reported_date - 3
                ELSE date(created_at - INTERVAL '4 hour') - 3
            END
        ) STORED,
    reported_date DATE NOT NULL
        GENERATED ALWAYS AS (
            CASE
                WHEN raw_reported_date >= '2020-03-13'
                THEN raw_reported_date
                ELSE date(created_at - INTERVAL '4 hour')
            END
        ) STORED,
    created_date DATE NOT NULL
        GENERATED ALWAYS AS (
            date(created_at - INTERVAL '4 hour')
        ) STORED,

    patient_id UUID,
    age_range TEXT,

    raw_municipality TEXT,
    municipality TEXT
        GENERATED ALWAYS AS (
                CASE raw_municipality
                WHEN 'Rio Grande' THEN 'Río Grande'
                ELSE raw_municipality
                END
        ) STORED,

    test_type TEXT,
    result TEXT,
    positive BOOLEAN NOT NULL
        GENERATED ALWAYS AS (
            COALESCE(result, '') LIKE '%Positive%'
        ) STORED,
    PRIMARY KEY (id)
);


CREATE MATERIALIZED VIEW bioportal_tritemporal_counts AS
SELECT
	test_type,
	bulletin_date,
	reported_date,
	collected_date,
	count(*) tests,
	count(*) FILTER (WHERE positive)
		AS positive_tests
FROM bioportal_tests
WHERE '2020-03-01' <= collected_date
AND collected_date <= bulletin_date
AND '2020-03-01' <= reported_date
AND reported_date <= bulletin_date
GROUP BY test_type, bulletin_date, collected_date, reported_date;


CREATE MATERIALIZED VIEW bioportal_tritemporal_deltas AS
SELECT
	test_type,
	bulletin_date,
	reported_date,
	collected_date,
	tests,
	tests - COALESCE(lag(tests) OVER bulletin, 0)
		AS delta_tests,
	positive_tests,
	positive_tests - COALESCE(lag(positive_tests) OVER bulletin, 0)
		AS delta_positive_tests
FROM bioportal_tritemporal_counts
WHERE collected_date <= bulletin_date
AND reported_date <= bulletin_date
WINDOW bulletin AS (
	PARTITION BY test_type, collected_date, reported_date
	ORDER BY bulletin_date
);

CREATE MATERIALIZED VIEW bioportal_collected_agg AS
SELECT
	test_type,
	bulletin_date,
	collected_date,
	bulletin_date - collected_date
		AS collected_age,
	sum(tests) AS tests,
	sum(sum(tests)) OVER seven / 7.0
		AS smoothed_daily_tests,
	sum(sum(tests)) OVER cumulative
		AS cumulative_tests,
	sum(delta_tests) AS delta_tests,
	sum(positive_tests) AS positive_tests,
	sum(sum(positive_tests)) OVER seven / 7.0
		AS smoothed_daily_positive_tests,
	sum(sum(positive_tests)) OVER cumulative
		AS cumulative_positive_tests,
	sum(delta_positive_tests) AS delta_positive_tests
FROM bioportal_tritemporal_deltas
GROUP BY test_type, bulletin_date, collected_date
WINDOW cumulative AS (
	PARTITION BY test_type, bulletin_date
	ORDER BY collected_date
), seven AS (
	PARTITION BY test_type, bulletin_date
	ORDER BY collected_date
	RANGE '6 day' PRECEDING
);

CREATE MATERIALIZED VIEW bioportal_reported_agg AS
SELECT
	test_type,
	bulletin_date,
	reported_date,
	bulletin_date - reported_date
		AS collected_age,
	sum(tests) AS tests,
	sum(sum(tests)) OVER seven / 7.0
		AS smoothed_daily_tests,
	sum(sum(tests)) OVER cumulative
		AS cumulative_tests,
	sum(delta_tests) AS delta_tests,
	sum(positive_tests) AS positive_tests,
	sum(sum(positive_tests)) OVER seven / 7.0
		AS smoothed_daily_positive_tests,
	sum(sum(positive_tests)) OVER cumulative
		AS cumulative_positive_tests,
	sum(delta_positive_tests) AS delta_positive_tests
FROM bioportal_tritemporal_deltas
GROUP BY test_type, bulletin_date, reported_date
WINDOW cumulative AS (
	PARTITION BY test_type, bulletin_date
	ORDER BY reported_date
), seven AS (
	PARTITION BY test_type, bulletin_date
	ORDER BY reported_date
	RANGE '6 day' PRECEDING
);


-----------------------------------------------------------------------------

CREATE VIEW products.new_daily_tests AS
SELECT
    'Fecha de muestra' AS date_type,
    test_type,
    bulletin_date,
    collected_date AS date,
    smoothed_daily_tests
FROM bioportal_collected_agg
UNION
SELECT
    'Fecha de reporte' AS date_type,
    test_type,
    bulletin_date,
    reported_date AS date,
    smoothed_daily_tests
FROM bioportal_reported_agg;

CREATE VIEW products.positive_rates AS
SELECT
	molecular.test_type,
	molecular.bulletin_date,
	collected_date,
	molecular.smoothed_daily_tests,
	molecular.smoothed_daily_positive_tests,
	(cumulative_confirmed_cases
		- LAG(cumulative_confirmed_cases, 7, 0::bigint) OVER seven)
		/ 7.0
		AS smoothed_daily_cases
FROM bioportal_collected_agg molecular
INNER JOIN bitemporal_agg cases
	ON cases.bulletin_date = molecular.bulletin_date
	AND cases.datum_date = molecular.collected_date
WHERE molecular.test_type = 'Molecular'
AND molecular.bulletin_date > '2020-04-24'
WINDOW seven AS (
	PARTITION BY molecular.bulletin_date
	ORDER BY collected_date
	RANGE '6 days' PRECEDING
)

UNION

SELECT
	serological.test_type,
	serological.bulletin_date,
	collected_date,
	serological.smoothed_daily_tests,
	serological.smoothed_daily_positive_tests,
	(cumulative_probable_cases
		- LAG(cumulative_probable_cases, 7, 0::bigint) OVER seven)
		/ 7.0
		AS smoothed_daily_cases
FROM bioportal_collected_agg serological
INNER JOIN bitemporal_agg cases
	ON cases.bulletin_date = serological.bulletin_date
	AND cases.datum_date = serological.collected_date
WHERE serological.test_type = 'Serological'
AND serological.bulletin_date > '2020-04-24'
WINDOW seven AS (
	PARTITION BY serological.bulletin_date
	ORDER BY collected_date
	RANGE '6 days' PRECEDING
);


CREATE VIEW products.molecular_tests_vs_confirmed_cases AS
SELECT
	tests.bulletin_date,
	collected_date,
	cumulative_tests,
	cumulative_positive_tests,
	cumulative_confirmed_cases
	    AS cumulative_cases,
	smoothed_daily_tests,
	smoothed_daily_positive_tests,
	(cumulative_confirmed_cases
		- LAG(cumulative_confirmed_cases, 7, 0::bigint) OVER seven)
		/ 7.0
		AS smoothed_daily_cases
FROM bioportal_collected_agg tests
INNER JOIN bitemporal_agg cases
	ON cases.bulletin_date = tests.bulletin_date
	AND cases.datum_date = tests.collected_date
WHERE tests.bulletin_date > '2020-04-24'
AND test_type = 'Molecular'
WINDOW seven AS (
	PARTITION BY tests.bulletin_date
	ORDER BY collected_date
	RANGE '6 days' PRECEDING
);


CREATE VIEW products.molecular_lateness AS
SELECT
    bulletin_date,
    test_type,
    safediv(sum(delta_tests * collected_age),
            sum(delta_tests))
        AS lateness_tests,
    safediv(sum(delta_positive_tests * collected_age),
            sum(delta_positive_tests))
        AS lateness_positive_tests,
    safediv(sum(sum(delta_tests * collected_age)) OVER seven,
            sum(sum(delta_tests)) OVER seven)
        AS smoothed_lateness_tests,
    safediv(sum(sum(delta_positive_tests * collected_age)) OVER seven,
            sum(sum(delta_positive_tests)) OVER seven)
        AS smoothed_lateness_positive_tests
FROM bioportal_collected_agg
GROUP BY bulletin_date, test_type
WINDOW seven AS (
	ORDER BY bulletin_date
	RANGE '6 days' PRECEDING
);


CREATE VIEW products.testing_load AS
SELECT
	bca.bulletin_date,
	bca.collected_date,
	avg(bca.tests) OVER collected AS tests,
	avg(bca.tests - bca.positive_tests)
		OVER collected
		AS negatives,
	avg(COALESCE(b.confirmed_cases, 0))
		OVER collected
		AS new_cases,
	avg(bca.positive_tests - COALESCE(b.confirmed_cases, 0))
		OVER collected
		AS duplicate_positives
FROM bioportal_collected_agg bca
INNER JOIN bitemporal b
	ON b.bulletin_date = bca.bulletin_date
	AND b.datum_date = bca.collected_date
WHERE test_type = 'Molecular'
WINDOW collected AS (
	PARTITION BY test_type, bca.bulletin_date
	ORDER BY collected_date RANGE '6 day' PRECEDING
);