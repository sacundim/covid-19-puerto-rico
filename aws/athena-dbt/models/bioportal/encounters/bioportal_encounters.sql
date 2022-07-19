----------------------------------------------------------
----------------------------------------------------------
--
-- # Encounters and followup analysis
--
-- This table takes all the antigen and PCR tests (no serology)
-- and does the following cleanup and enrichment:
--
-- 1. Eliminates duplicate tests for the same patient on the
--    same date. If any of the tests on one day is positive,
--    we classify the patient as a positive on that day.  We
--    call these "test encounters," a term used by the COVID
--    Tracking Project.
--
-- See: https://covidtracking.com/analysis-updates/test-positivity-in-the-us-is-a-mess
--
-- 2. Keep track of how many days have passed since the patient's
--    most recent positive test encounter, which is used to decide
--    heuristically whether to count a positive is as a reinfection
--    or as a positive result to a followup of the case case.
--

WITH downloads AS (
	SELECT
		max(downloaded_at) max_downloaded_at,
		max(downloaded_date) max_downloaded_date
	FROM {{ ref('bioportal_orders_basic') }}
), ordered AS (
    SELECT
        cur.downloaded_at,
        cur.downloaded_date,
        cur.collected_date,
        cur.patient_id,
        max_by(cur.age_range, cur.received_date) AS age_range,
        max_by(cur.region, cur.received_date) AS region,
        min(cur.received_date) AS min_received_date,
        max(cur.received_date) AS max_received_date,
        -- True if and only if at least one specimen for that
        -- patient on that date came back positive, irrespective
        -- of the type of test.
        bool_or(cur.positive) positive,
        -- A first infection is the first positive encounter a patient has ever had.
        bool_or(cur.positive)
            AND COALESCE(NOT bool_or(prev.positive), TRUE)
            AS first_infection,
        -- These two are true if and only if there is at least one
        -- specimen for that patient on that date was of the respective
        -- type, irrespective of positive and negative.
        bool_or(cur.test_type = 'Molecular') has_molecular,
        bool_or(cur.test_type = 'Antígeno') has_antigens,
        -- These two true are if and only if there is at least one
        -- specimen that is both of the respective type and came
        -- back positive.  Note that for example this means that
        -- `has_positive_antigens` isn't synonymous with
        -- `positive AND has_antigens`, because that could be true
        -- because the patient has a negative antigen and a positive
        -- molecular test on that date.
        bool_or(cur.test_type = 'Molecular' AND cur.positive)
            AS has_positive_molecular,
        bool_or(cur.test_type = 'Antígeno' AND cur.positive)
            AS has_positive_antigens,
        -- How many days have passed between this encounted and the most recent
        -- encounter that had a positive test.  Null if there isn't one.
        min(date_diff('day', prev.collected_date, cur.collected_date)) FILTER (
            WHERE prev.positive
        ) days_since_previous_positive
    FROM {{ ref('bioportal_orders_basic') }} cur
    INNER JOIN downloads
        ON cur.downloaded_at = downloads.max_downloaded_at
        AND cur.downloaded_date = downloads.max_downloaded_date
    LEFT OUTER JOIN {{ ref('bioportal_orders_basic') }} prev
        ON prev.test_type IN ('Molecular', 'Antígeno')
        AND prev.downloaded_at = cur.downloaded_at
        AND prev.downloaded_date = cur.downloaded_date
        AND prev.patient_id = cur.patient_id
        AND prev.collected_date < cur.collected_date
        AND DATE '2020-03-01' <= prev.collected_date
        AND prev.collected_date <= prev.received_date
        AND DATE '2020-03-01' <= prev.reported_date
        AND prev.reported_date <= prev.received_date
    WHERE cur.test_type IN ('Molecular', 'Antígeno')
    AND DATE '2020-03-01' <= cur.collected_date
    AND cur.collected_date <= cur.received_date
    AND DATE '2020-03-01' <= cur.reported_date
    AND cur.reported_date <= cur.received_date
    GROUP BY
        cur.downloaded_at,
        cur.downloaded_date,
        cur.collected_date,
        cur.patient_id
)
SELECT
    *,
    -- A testing encounter is eligible to be counted as a new case if and only one of the
    -- following is met:
    --
    -- 1. There is no earlier known positive encounter with that patient;
    -- 2. The encounter had  an antigen test and it is at least 21 days since the patient's
    --    most recent positive encounter;
    -- 3. The encounter had a molecular test and is at least 90 days since the patient's
    --    most recent positive encounter.
    --
    -- We used to compute the negation of this and call it a "followup encounter," but this
    -- present formulation is wieldier.  E.g., `is_case / can_be_case` is a potentially more
    -- principled way of calculating positivity than more common, lazier approaches.
    --
    days_since_previous_positive IS NULL
        OR (has_antigens AND days_since_previous_positive >= 21)
        OR (has_molecular AND days_since_previous_positive >= 90)
        AS can_be_case,

    -- A case is a positive encounter that is either the patient's first such encounter or:
    --
    -- 1. Has a positive antigens test and is at least 21 days from their previous
    --    positive encounter;
    -- 2. Has a positive molecular test and is at least 90 days from their previous
    --    positive encounter.
    --
    first_infection
        OR (has_positive_antigens AND days_since_previous_positive >= 21)
        OR (has_positive_molecular AND days_since_previous_positive >= 90)
        AS is_case
FROM ordered;
