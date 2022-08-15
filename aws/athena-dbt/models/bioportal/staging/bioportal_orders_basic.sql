--
-- The `orders/basic` row-per-test dataset from Bioportal.
--

{{
    config(pre_hook=[
        "MSCK REPAIR TABLE {{ source('bioportal', 'orders_basic_v2') }}",
        "MSCK REPAIR TABLE {{ source('bioportal', 'orders_basic_v5') }}"
    ])
}}

WITH downloads AS (
	SELECT
		max(downloaded_date) max_downloaded_date,
		max("$path") max_path
	FROM {{ source('bioportal', 'orders_basic_v5') }}
), first_clean AS (
	SELECT
	    {{ parse_filename_timestamp('tests."$path"') }}
	        AS downloaded_at,
	    CAST(from_iso8601_timestamp(nullif(collectedDate, '')) AS TIMESTAMP)
	    	AS raw_collected_at,
	    CAST(from_iso8601_timestamp(nullif(reportedDate, '')) AS TIMESTAMP)
	    	AS raw_reported_at,
	    CAST(from_iso8601_timestamp(resultCreatedAt) AS TIMESTAMP)
	    	AS result_created_at,
	    CAST(from_iso8601_timestamp(orderCreatedAt) AS TIMESTAMP)
	    	AS order_created_at,
	    date({{ parse_filename_timestamp('tests."$path"') }} AT TIME ZONE 'America/Puerto_Rico')
	        AS downloaded_date,
	    date(from_iso8601_timestamp(resultCreatedAt) AT TIME ZONE 'America/Puerto_Rico')
	        AS received_date,
	    date(from_iso8601_timestamp(nullif(collectedDate, '')) AT TIME ZONE 'America/Puerto_Rico')
	        AS raw_collected_date,
	    date(from_iso8601_timestamp(nullif(reportedDate, '')) AT TIME ZONE 'America/Puerto_Rico')
	        AS raw_reported_date,
	    from_hex(replace(nullif(patientId, ''), '-')) AS patient_id,
        {{ clean_age_range('ageRange') }} AS age_range,
        {{ clean_region('region') }} AS region,
	    nullif(testType, '') AS raw_test_type,
	    {{ clean_test_type('testType') }} AS test_type,
	    nullif(result, '') result,
        {{ parse_bioportal_result('result', 'positive') }} AS positive
	FROM {{ source('bioportal', 'orders_basic_v5') }} tests
	INNER JOIN downloads
	    ON downloads.max_path = tests."$path"
	    -- This is redundant but it seems to prune how much data is scanned
	    AND downloads.max_downloaded_date = tests.downloaded_date
    LEFT OUTER JOIN {{ ref('expected_test_results') }} results
        USING (result)
    -- IMPORTANT: This prunes partitions to just the very most recent ones
    WHERE downloaded_date >= cast(date_add('day', -1, current_date) AS VARCHAR)
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
	    ELSE coalesce(raw_collected_date, raw_reported_date, received_date)
    END AS collected_date,
    CASE
	    WHEN test_type IN ('Molecular')
	    THEN CASE
	        WHEN raw_reported_date >= DATE '2020-03-13'
	        THEN raw_reported_date
	        ELSE received_date
	    END
	    WHEN test_type IN ('Antígeno')
	    THEN greatest(coalesce(raw_collected_date, DATE '{{ var("beginning_of_time") }}'),
	                  coalesce(raw_reported_date, DATE '{{ var("beginning_of_time") }}'))
	    ELSE coalesce(raw_reported_date, raw_collected_date, received_date)
    END AS reported_date
FROM first_clean;
