------------------------------------------------------------------------
------------------------------------------------------------------------
--
-- The `minimal-info-unique-tests` row-per-test dataset.
-- This doesn't have the patient_id field, so we can't
-- identify cases from it, but it does have municipalities.
--

{{
    config(pre_hook=[
        "MSCK REPAIR TABLE {{ source('bioportal', 'minimal_info_unique_tests_v4') }}",
        "MSCK REPAIR TABLE {{ source('bioportal', 'minimal_info_unique_tests_v5') }}"
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
        CAST(date_parse(nullif(collectedDate, ''), '%m/%d/%Y') AS DATE)
            AS raw_collected_date,
        CAST(date_parse(nullif(reportedDate, ''), '%m/%d/%Y') AS DATE)
            AS raw_reported_date,
        date_parse(createdAt, '%m/%d/%Y %H:%i') AS created_at,
        {{ clean_age_range('ageRange') }} AS age_range,
        {{ clean_municipality('city') }} AS municipality,
	    nullif(testType, '') AS raw_test_type,
	    {{ clean_test_type('testType') }} AS test_type,
	    nullif(result, '') result,
        {{ parse_bioportal_result('result', 'positive') }} AS positive
    FROM {{ source('bioportal', 'minimal_info_unique_tests_v4') }} tests
    LEFT OUTER JOIN {{ ref('expected_test_results') }} results
        USING (result)
    -- IMPORTANT: This prunes partitions
    WHERE downloaded_date >= cast(date_add('day', -32, current_date) AS VARCHAR)

    UNION ALL

    SELECT
    	date(downloaded_date) AS downloaded_date,
        {{ parse_filename_timestamp('tests."$path"') }}
            AS downloaded_at,
        CAST({{ parse_filename_timestamp('tests."$path"') }} AT TIME ZONE 'America/Puerto_Rico' AS DATE)
            - INTERVAL '1' DAY
            AS bulletin_date,
        CAST(date_parse(nullif(collectedDate, ''), '%m/%d/%Y') AS DATE)
            AS raw_collected_date,
        CAST(date_parse(nullif(reportedDate, ''), '%m/%d/%Y') AS DATE)
            AS raw_reported_date,
        date_parse(createdAt, '%m/%d/%Y %H:%i') AS created_at,
        {{ clean_age_range('ageRange') }} AS age_range,
        {{ clean_municipality('city') }} AS municipality,
	    nullif(testType, '') AS raw_test_type,
	    {{ clean_test_type('testType') }} AS test_type,
	    nullif(result, '') result,
        {{ parse_bioportal_result('result', 'positive') }} AS positive
    FROM {{ source('bioportal', 'minimal_info_unique_tests_v5') }} tests
    LEFT OUTER JOIN {{ ref('expected_test_results') }} results
        USING (result)
    -- IMPORTANT: This prunes partitions
    WHERE downloaded_date >= cast(date_add('day', -32, current_date) AS VARCHAR)
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
FROM first_clean;
