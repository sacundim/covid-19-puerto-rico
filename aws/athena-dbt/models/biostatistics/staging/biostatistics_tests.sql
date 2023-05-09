{{
    config(pre_hook=[
        "MSCK REPAIR TABLE {{ source('biostatistics', 'tests_v1').render_hive() }}"
    ])
}}
WITH first_clean AS (
    SELECT
        date(downloaded_date) AS downloaded_date,
        {{ parse_filename_timestamp('tests."$path"') }}
            AS downloaded_at,
        CAST({{ parse_filename_timestamp('tests."$path"') }} AT TIME ZONE 'America/Puerto_Rico' AS DATE)
            - INTERVAL '1' DAY
            AS bulletin_date,
        orderTestId AS order_test_id,
        patientId as patient_id,
        {{ clean_age_range('patientAgeRange') }} AS age_range,
        {{ clean_region('patientRegion') }} AS region,
        {{ clean_municipality('patientCity') }} AS municipality,
	    nullif(orderTestType, '') AS raw_test_type,
	    {{ clean_test_type('orderTestType') }} AS test_type,
	    {{ clean_utc_timestamp('sampleCollectedDate') }}
            AS raw_collected_utc,
        {{ utc_to_pr_date('sampleCollectedDate') }}
            AS raw_collected_date,
	    {{ clean_utc_timestamp('resultReportDate') }}
            AS raw_result_report_utc,
        {{ utc_to_pr_date('resultReportDate') }}
            AS raw_reported_date,
	    nullif(orderTestResult, '') AS result,
        {{ parse_bioportal_result('orderTestResult', 'results.positive') }}
            AS positive,
	    {{ clean_utc_timestamp('orderTestCreatedAt') }}
            AS created_at_utc
    FROM {{ source('biostatistics', 'tests_v1') }} tests
    LEFT OUTER JOIN {{ ref('expected_test_results') }} results
        ON tests.orderTestResult = results.result
    WHERE orderTestCategory IN ('Covid-19')
    -- IMPORTANT: This prunes partitions
    AND downloaded_date >= cast(date_add('day', -32, current_date) AS VARCHAR)
)
SELECT
    *,
    CASE
	    WHEN test_type IN ('Molecular')
	    THEN CASE
	        -- Null out nonsense collected dates. As of 2021-04-15,
	        -- out of over 1.9M PCR records there were only 267 with
	        -- `raw_collected_date` earlier than March 1 2020 and
	        -- 2,658 with nulls, so we don't really lose much.
	        WHEN raw_collected_date >= DATE '2020-03-01'
	        THEN raw_collected_date
	        -- This was the original method I used to clean up null
	        -- `collected_date` values, but now only for very early
	        -- dates.  Suggested originally by @rafalab; he uses two
	        -- days as the value and says that's the average, but my
	        -- spot check said 2.8 days so I use that.
	        WHEN DATE '2020-03-13' <= raw_reported_date
	                AND raw_reported_date <= DATE '2020-07-01'
	        THEN date_add('day', -3, raw_reported_date)
	    END
	    WHEN test_type IN ('Antígeno')
	    -- As of 2021-04-15, out of 652k antigen test records,
	    -- over 32k have `raw_collected_date` > `raw_reported_date`,
	    -- generally off by one day but some by a handful.  A lot of
	    -- the `raw_collected_date` look handwritten and approximate
	    -- like `2021-02-03 11:25:00` (five minute increments, no
	    -- seconds) while the `raw_reported_date` ones look computer
	    -- generated (second precision values).  I'm going to assume
	    -- that whichever of the two dates is earlier is likelier to
	    -- be right.
	    THEN least(coalesce(raw_collected_date, DATE '{{ var("end_of_time") }}'),
	               coalesce(raw_reported_date, DATE '{{ var("end_of_time") }}'))
	    ELSE coalesce(raw_collected_date, raw_reported_date, downloaded_date)
    END AS collected_date,
    CASE
	    WHEN test_type IN ('Molecular')
	    THEN CASE
	        WHEN raw_reported_date >= DATE '2020-03-13'
	        THEN raw_reported_date
	        ELSE downloaded_date
	    END
	    WHEN test_type IN ('Antígeno')
	    THEN greatest(coalesce(raw_collected_date, DATE '{{ var("beginning_of_time") }}'),
	                  coalesce(raw_reported_date, DATE '{{ var("beginning_of_time") }}'))
	    ELSE coalesce(raw_reported_date, raw_collected_date, downloaded_date)
    END AS reported_date
FROM first_clean
ORDER BY downloaded_at, raw_collected_utc;