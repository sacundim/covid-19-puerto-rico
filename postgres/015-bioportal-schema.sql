CREATE FUNCTION to_pr_date(x TIMESTAMP WITH TIME ZONE)
RETURNS DATE AS $$
    -- Puerto Rico is UTC-4 all year long
    SELECT date(x AT TIME ZONE 'America/Puerto_Rico');
$$ LANGUAGE SQL IMMUTABLE;

CREATE FUNCTION clean_collected_date(test_type TEXT,
                                     raw_collected_date TIMESTAMP WITH TIME ZONE,
                                     raw_reported_date TIMESTAMP WITH TIME ZONE,
                                     result_created_at TIMESTAMP WITH TIME ZONE)
RETURNS DATE AS $$
    SELECT CASE
                WHEN test_type IN ('Molecular')
                THEN CASE
                    WHEN to_pr_date(raw_collected_date) >= DATE '2020-01-01'
                    THEN to_pr_date(raw_collected_date)
                    WHEN to_pr_date(raw_reported_date) >= DATE '2020-03-13'
                    -- Suggested by @rafalab. He uses two days as the value and says
                    -- that's the average, but my spot check says 2.8 days.
                    THEN to_pr_date(raw_reported_date) - 3
                    ELSE to_pr_date(result_created_at) - 3
                END
                ELSE coalesce(to_pr_date(raw_collected_date),
                              to_pr_date(raw_reported_date),
                              to_pr_date(result_created_at))
            END;
$$ LANGUAGE SQL IMMUTABLE;

CREATE FUNCTION clean_reported_date(test_type TEXT,
                                    raw_collected_date TIMESTAMP WITH TIME ZONE,
                                    raw_reported_date TIMESTAMP WITH TIME ZONE,
                                    result_created_at TIMESTAMP WITH TIME ZONE)
RETURNS DATE AS $$
    SELECT CASE
                WHEN test_type IN ('Molecular')
                THEN CASE
                    WHEN to_pr_date(raw_reported_date) >= DATE '2020-03-13'
                    THEN to_pr_date(raw_reported_date)
                    ELSE to_pr_date(result_created_at)
                END
                ELSE coalesce(to_pr_date(raw_reported_date),
                              to_pr_date(raw_collected_date),
                              to_pr_date(result_created_at))
            END;
$$ LANGUAGE SQL IMMUTABLE;


CREATE UNLOGGED TABLE bioportal_tests (
    id SERIAL NOT NULL,
    downloaded_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    patient_id UUID NOT NULL,
    raw_collected_date TIMESTAMP WITH TIME ZONE,
    raw_reported_date TIMESTAMP WITH TIME ZONE,
    age_range TEXT,
    test_type TEXT NOT NULL,
    result TEXT NOT NULL,
    region TEXT,
    order_created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    result_created_at TIMESTAMP WITH TIME ZONE NOT NULL,

    bulletin_date DATE NOT NULL
        GENERATED ALWAYS AS (
            to_pr_date(result_created_at)
        ) STORED,
    collected_date DATE NOT NULL
        GENERATED ALWAYS AS (
            clean_collected_date(test_type, raw_collected_date, raw_reported_date, result_created_at)
        ) STORED,
    reported_date DATE
        GENERATED ALWAYS AS (
            clean_reported_date(test_type, raw_collected_date, raw_reported_date, result_created_at)
        ) STORED,
    positive BOOLEAN NOT NULL
        GENERATED ALWAYS AS (
            COALESCE(result, '') LIKE '%Positive%'
        ) STORED,

    PRIMARY KEY (id)
);


CREATE MATERIALIZED VIEW bioportal_tritemporal_preagg AS
SELECT
	test_type,
	bulletin_date,
	collected_date,
	reported_date,
	count(*) delta_tests,
	count(*) FILTER (WHERE positive)
		AS delta_positive_tests
FROM bioportal_tests
WHERE '2020-03-01' <= collected_date
AND collected_date <= bulletin_date
AND '2020-03-01' <= reported_date
AND reported_date <= bulletin_date
GROUP BY test_type, bulletin_date, collected_date, reported_date;


CREATE MATERIALIZED VIEW bioportal_tritemporal_agg AS
WITH test_types AS (
	SELECT DISTINCT test_type
	FROM bioportal_tritemporal_preagg
), bulletin_dates AS (
	SELECT DISTINCT bulletin_date
	FROM bioportal_tritemporal_preagg
), collected_dates AS (
	SELECT DISTINCT collected_date
	FROM bioportal_tritemporal_preagg
), reported_dates AS (
	SELECT DISTINCT reported_date
	FROM bioportal_tritemporal_preagg
)
SELECT
	test_types.test_type,
	bulletin_dates.bulletin_date,
	collected_dates.collected_date,
	reported_dates.reported_date,
	btpa.delta_tests,
	btpa.delta_positive_tests,
	sum(btpa.delta_tests)
		OVER bulletins
		AS tests,
	sum(btpa.delta_positive_tests)
		OVER bulletins
		AS positive_tests
FROM test_types
CROSS JOIN bulletin_dates
INNER JOIN reported_dates
	ON reported_dates.reported_date <= bulletin_dates.bulletin_date
INNER JOIN collected_dates
	ON collected_dates.collected_date <= reported_dates.reported_date
LEFT OUTER JOIN bioportal_tritemporal_preagg btpa
	ON btpa.test_type = test_types.test_type
	AND btpa.bulletin_date = bulletin_dates.bulletin_date
	AND btpa.collected_date = collected_dates.collected_date
	AND btpa.reported_date = reported_dates.reported_date
WINDOW bulletins AS (
	PARTITION BY
		test_types.test_type,
		collected_dates.collected_date,
		reported_dates.reported_date
	ORDER BY bulletin_dates.bulletin_date
)
ORDER BY
	btpa.test_type,
	bulletin_dates.bulletin_date,
	collected_dates.collected_date,
	reported_dates.reported_date;



CREATE MATERIALIZED VIEW bioportal_collected_agg AS
SELECT
	test_type,
	bulletin_date,
	collected_date,
	bulletin_date - collected_date
		AS collected_age,
	sum(tests) AS tests,
	sum(sum(tests)) OVER (
        PARTITION BY test_type, bulletin_date
        ORDER BY collected_date
    ) AS cumulative_tests,
	sum(delta_tests) AS delta_tests,
	sum(positive_tests) AS positive_tests,
	sum(sum(positive_tests)) OVER (
        PARTITION BY test_type, bulletin_date
        ORDER BY collected_date
    ) AS cumulative_positives,
	sum(delta_positive_tests) AS delta_positive_tests
FROM bioportal_tritemporal_agg
GROUP BY test_type, bulletin_date, collected_date
ORDER BY test_type, bulletin_date, collected_date;


CREATE MATERIALIZED VIEW bioportal_reported_agg AS
SELECT
	test_type,
	bulletin_date,
	reported_date,
	bulletin_date - reported_date
		AS reported_age,
	sum(tests) AS tests,
	sum(sum(tests)) OVER (
        PARTITION BY test_type, bulletin_date
        ORDER BY reported_date
    ) AS cumulative_tests,
	sum(delta_tests) AS delta_tests,
	sum(positive_tests) AS positive_tests,
	sum(sum(positive_tests)) OVER (
        PARTITION BY test_type, bulletin_date
        ORDER BY reported_date
    ) AS cumulative_positives,
	sum(delta_positive_tests) AS delta_positive_tests
FROM bioportal_tritemporal_agg
GROUP BY test_type, bulletin_date, reported_date
ORDER BY test_type, bulletin_date, reported_date;