--
-- The "downloads cube" has aggregates for all the file downloads,
-- which may be more than one for each bulletin_date.  This is not
-- what we usually want but the upside is we can update it incrementally
--
{{
    config(
        table_type='iceberg',
        partitioned_by=['month(downloaded_at)'],
        materialized='incremental',
        incremental_strategy='append',
        post_hook = [
            'VACUUM {{ this.render_pure() }};'
        ]
    )
}}
SELECT
    downloaded_at,
    bulletin_date,
    collected_date,
    municipality,
    age_range,
    age_gte,
    age_lt,
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
FROM {{ ref('biostatistics_encounters') }}
INNER JOIN {{ ref('bioportal_age_ranges') }}
    USING (age_range)
{% if is_incremental() %}
WHERE downloaded_at > (SELECT max(downloaded_at) FROM {{ this }})
{% endif %}
GROUP BY
    downloaded_at,
    bulletin_date,
    collected_date,
    municipality,
    age_range,
    age_gte,
    age_lt