WITH downloads AS (
	SELECT
		max(downloaded_at) max_downloaded_at,
		max(downloaded_date) max_downloaded_date
	FROM {{ ref('bioportal_orders_basic') }}
), bulletins AS (
	SELECT CAST(date_column AS DATE) AS bulletin_date
	FROM (
		VALUES (SEQUENCE(DATE '{{ var("first_bulletin_date") }}', DATE '{{ var("end_date") }}', INTERVAL '1' DAY))
	) AS date_array (date_array)
	CROSS JOIN UNNEST(date_array) AS t2(date_column)
	INNER JOIN downloads
	    ON CAST(date_column AS DATE) < downloads.max_downloaded_date
), grouped AS (
    SELECT
        bulletins.bulletin_date,
        tests.collected_date,
        tests.age_range,
        count(*) AS encounters,
        -- A case is a test encounter that had a positive test and
        -- is not a followup to an earlier positive encounter.
        count(*) FILTER (
            WHERE tests.positive
            AND NOT tests.followup
        ) cases,
        -- An antigens case is a test encounter that had a positive antigens
        -- test and is not a followup to an earlier positive encounter.
        count(*) FILTER (
            WHERE tests.has_positive_antigens
            AND NOT tests.followup
        ) antigens_cases,
        -- A molecular case is a test encounter that had a positive PCR
        -- test, no positive antigen test, and is not a followup to an
        -- earlier positive encounter.
        count(*) FILTER (
            WHERE tests.has_positive_molecular
            AND NOT tests.has_positive_antigens
            AND NOT tests.followup
        ) molecular_cases,
        -- A rejected case is a non-followup encounter that had no
        -- positive tests and at least one of the tests is PCR.
        count(*) FILTER (
            WHERE tests.has_molecular
            AND NOT tests.followup
            AND NOT tests.positive
        ) rejections,
        -- Note that `has_antigens` and `has_molecular` don't
        -- have to add up to `encounters` because a person may
        -- get both test types the same day. Similar remarks
        -- apply to many of the sums below.
        count(*) FILTER (
            WHERE tests.has_antigens
        ) antigens,
        count(*) FILTER (
            WHERE tests.has_molecular
        ) molecular,
        -- These two are encounters where there was at least one
        -- positive test of the respective type.  Note that for
        -- example `has_positive_antigens` isn't synonymos with
        -- `positive AND has_antigens`, because a patient could
        -- have a negative antigen and a positive PCR the same day.
        count(*) FILTER (
            WHERE tests.has_positive_antigens
        ) positive_antigens,
        count(*) FILTER (
            WHERE tests.has_positive_molecular
        ) positive_molecular,
        -- Non-followup test encounters where there was at least
        -- one molecular test.  These are, I claim, the most
        -- appropriate for a positive rate calculation.
        count(*) FILTER (
            WHERE NOT tests.followup
            AND tests.has_molecular
        ) AS initial_molecular,
        -- Non-followup test encounters where there was at least
        -- one molecular test that came back positive.  These are,
        -- I claim, the most appropriate for a positive rate calculation.
        count(*) FILTER (
            WHERE NOT tests.followup
            AND tests.has_positive_molecular
        ) AS initial_positive_molecular
    FROM {{ ref('bioportal_encounters') }} tests
    INNER JOIN downloads
        ON tests.downloaded_at = downloads.max_downloaded_at
    INNER JOIN bulletins
        ON bulletins.bulletin_date < downloads.max_downloaded_date
        AND tests.min_received_date <= bulletins.bulletin_date
    GROUP BY
        bulletins.bulletin_date,
        tests.collected_date,
        tests.age_range
)
SELECT
    *,
    sum(encounters) OVER (
        PARTITION BY bulletin_date, age_range
        ORDER BY collected_date
    ) AS cumulative_encounters,
    sum(cases) OVER (
        PARTITION BY bulletin_date, age_range
        ORDER BY collected_date
    ) AS cumulative_cases,
    sum(rejections) OVER (
        PARTITION BY bulletin_date, age_range
        ORDER BY collected_date
    ) AS cumulative_rejections,
    sum(antigens) OVER (
        PARTITION BY bulletin_date, age_range
        ORDER BY collected_date
    ) AS cumulative_antigens,
    sum(molecular) OVER (
        PARTITION BY bulletin_date, age_range
        ORDER BY collected_date
    ) AS cumulative_molecular,
    sum(positive_antigens) OVER (
        PARTITION BY bulletin_date, age_range
        ORDER BY collected_date
    ) AS cumulative_positive_antigens,
    sum(positive_molecular) OVER (
        PARTITION BY bulletin_date, age_range
        ORDER BY collected_date
    ) AS cumulative_positive_molecular,
    sum(initial_molecular) OVER (
        PARTITION BY bulletin_date, age_range
        ORDER BY collected_date
    ) AS cumulative_initial_molecular,
    sum(antigens_cases) OVER (
        PARTITION BY bulletin_date, age_range
        ORDER BY collected_date
    ) AS cumulative_antigens_cases,
    sum(molecular_cases) OVER (
        PARTITION BY bulletin_date, age_range
        ORDER BY collected_date
    ) AS cumulative_molecular_cases,
    sum(initial_positive_molecular) OVER (
        PARTITION BY bulletin_date, age_range
        ORDER BY collected_date
    ) AS cumulative_initial_positive_molecular
FROM grouped
ORDER BY
	bulletin_date,
	collected_date,
	age_range;
