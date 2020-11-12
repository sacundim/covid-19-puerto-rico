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

CREATE MATERIALIZED VIEW bioportal_tritemporal_agg AS
SELECT
	test_type,
	bulletin_date,
	collected_date,
	reported_date,
	count(*) tests,
	count(*) FILTER (WHERE positive)
		AS positive_tests
FROM bioportal_tests
WHERE '2020-03-01' <= collected_date
AND collected_date <= bulletin_date
AND '2020-03-01' <= reported_date
AND reported_date <= bulletin_date
GROUP BY test_type, bulletin_date, collected_date, reported_date;