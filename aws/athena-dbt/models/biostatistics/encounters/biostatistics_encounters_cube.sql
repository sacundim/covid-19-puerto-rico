WITH grouped AS (
    SELECT
        bulletin_date,
        collected_date,
    	municipality,
        age_range,
        count(*) AS encounters,
        -- A case is a test encounter that had a positive test and
        -- is not a followup to an earlier positive encounter.
        count(*) FILTER (
            WHERE positive
            AND NOT followup
        ) cases,
        -- Older version of the cases definition that used a stricter followup
        -- criterion (counted an antigen test up to 90 days later as followup)
        count(*) FILTER (
            WHERE positive
            AND NOT followup_strict
        ) cases_strict,
        -- A first infection is a test encounter where the result was positive
        -- and that patient has never had a positive test encounter before.
        count(*) FILTER (
            WHERE first_infection
        ) first_infections,
        -- A possible infection is a case that's not a first infection.
        count(*) FILTER (
            WHERE positive
            AND NOT followup
            AND NOT first_infection
        ) possible_reinfections,
        -- An antigens case is a test encounter that had a positive antigens
        -- test and is not a followup to an earlier positive encounter.
        count(*) FILTER (
            WHERE has_positive_antigens
            AND NOT followup
        ) antigens_cases,
        -- A molecular case is a test encounter that had a positive PCR
        -- test, no positive antigen test, and is not a followup to an
        -- earlier positive encounter.
        count(*) FILTER (
            WHERE has_positive_molecular
            AND NOT has_positive_antigens
            AND NOT followup
        ) molecular_cases,
        -- A rejected case is a non-followup encounter that had no
        -- positive tests and at least one of the tests is PCR.
        count(*) FILTER (
            WHERE has_molecular
            AND NOT followup
            AND NOT positive
        ) rejections,
        -- Note that `has_antigens` and `has_molecular` don't
        -- have to add up to `encounters` because a person may
        -- get both test types the same day. Similar remarks
        -- apply to many of the sums below.
        count(*) FILTER (
            WHERE has_antigens
        ) antigens,
        count(*) FILTER (
            WHERE has_molecular
        ) molecular,
        -- These two are encounters where there was at least one
        -- positive test of the respective type.  Note that for
        -- example `has_positive_antigens` isn't synonymos with
        -- `positive AND has_antigens`, because a patient could
        -- have a negative antigen and a positive PCR the same day.
        count(*) FILTER (
            WHERE has_positive_antigens
        ) positive_antigens,
        count(*) FILTER (
            WHERE has_positive_molecular
        ) positive_molecular,
        -- Non-followup test encounters where there was at least
        -- one molecular test.  These are, I claim, the most
        -- appropriate for a positive rate calculation.
        count(*) FILTER (
            WHERE NOT followup
            AND has_molecular
        ) AS initial_molecular,
        -- Non-followup test encounters where there was at least
        -- one molecular test that came back positive.  These are,
        -- I claim, the most appropriate for a positive rate calculation.
        count(*) FILTER (
            WHERE NOT followup
            AND has_positive_molecular
        ) AS initial_positive_molecular
    FROM {{ ref('biostatistics_encounters') }} tests
    GROUP BY
        bulletin_date,
        collected_date,
        municipality,
        age_range
)
SELECT
    *,
    sum(encounters) OVER collected
        AS cumulative_encounters,
    sum(cases) OVER collected
        AS cumulative_cases,
    sum(cases_strict) OVER collected
        AS cumulative_cases_strict,
    sum(first_infections) OVER collected
        AS cumulative_first_infections,
    sum(possible_reinfections) OVER collected
        AS cumulative_possible_reinfections,
    sum(rejections) OVER collected
        AS cumulative_rejections,
    sum(antigens) OVER collected
        AS cumulative_antigens,
    sum(molecular) OVER collected
        AS cumulative_molecular,
    sum(positive_antigens) OVER collected
        AS cumulative_positive_antigens,
    sum(positive_molecular) OVER collected
        AS cumulative_positive_molecular,
    sum(initial_molecular) OVER collected
        AS cumulative_initial_molecular,
    sum(antigens_cases) OVER collected
        AS cumulative_antigens_cases,
    sum(molecular_cases) OVER collected
        AS cumulative_molecular_cases,
    sum(initial_positive_molecular) OVER collected
        AS cumulative_initial_positive_molecular
FROM grouped
WINDOW collected AS (
    PARTITION BY bulletin_date, municipality, age_range
    ORDER BY collected_date
)
ORDER BY
	bulletin_date,
	collected_date,
	municipality,
	age_range;
