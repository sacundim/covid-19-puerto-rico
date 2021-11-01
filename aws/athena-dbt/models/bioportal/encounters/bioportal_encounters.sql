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
-- 2. Flags "followup" tests—tests such that the same patient
--    had a positive test no more than 90 days earlier. We use
--    a three month cutoff following the Council of State and
--    Territorial Epidemiologists (CSTE)'s 2020 Interim Case
--    Definition (Interim-20-ID-02, approved August 5, 2020),
--    which recommends this criterion for distinguishing new
--    cases from previous ones for the same patient.
--
-- See: https://wwwn.cdc.gov/nndss/conditions/coronavirus-disease-2019-covid-19/case-definition/2020/08/05/
--

WITH downloads AS (
	SELECT
		max(downloaded_at) max_downloaded_at,
		max(downloaded_date) max_downloaded_date
	FROM {{ ref('bioportal_orders_basic') }}
)
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
	-- These two are true if and only if there is at least one
	-- specimen for that patient on that date was of the respective
	-- type, irrespective of positive and negative.
	bool_or(cur.test_type = 'Molecular') has_molecular,
	bool_or(cur.test_type = 'Antígeno') has_antigens,
	-- These two true are if and only if there is at least one
	-- specimen that is both of the respective type and came
	-- back positive.  Note that for example this means that
	-- `has_positive_antigens` is't synonymous with
	-- `positive AND has_antigens`, because that could be true
	-- because the patient has a negative antigen and a positive
	-- molecular test on that date.
	bool_or(cur.test_type = 'Molecular' AND cur.positive)
	    AS has_positive_molecular,
	bool_or(cur.test_type = 'Antígeno' AND cur.positive)
	    AS has_positive_antigens,
    -- A followup test is any test—positive or negative—such
    -- that the same patient had a positive test in the 90
    -- days before.
	COALESCE(bool_or(prev.collected_date >= date_add('day', -90, cur.collected_date)
			            AND prev.positive),
             FALSE)
		AS followup
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
	cur.patient_id;
