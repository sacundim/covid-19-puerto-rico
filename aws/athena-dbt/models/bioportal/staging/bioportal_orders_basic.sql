--
-- The `orders/basic` row-per-test dataset from Bioportal.
--

WITH downloads AS (
	SELECT
		max(downloadedAt) max_downloaded_at
	FROM covid_pr_sources.orders_basic_parquet_v2
), first_clean AS (
	SELECT
	    CAST(from_iso8601_timestamp(downloadedAt) AS TIMESTAMP)
	        AS downloaded_at,
	    CAST(from_iso8601_timestamp(nullif(collectedDate, '')) AS TIMESTAMP)
	    	AS raw_collected_at,
	    CAST(from_iso8601_timestamp(nullif(reportedDate, '')) AS TIMESTAMP)
	    	AS raw_reported_at,
	    CAST(from_iso8601_timestamp(resultCreatedAt) AS TIMESTAMP)
	    	AS result_created_at,
	    CAST(from_iso8601_timestamp(orderCreatedAt) AS TIMESTAMP)
	    	AS order_created_at,
	    date(from_iso8601_timestamp(downloadedAt) AT TIME ZONE 'America/Puerto_Rico')
	        AS downloaded_date,
	    date(from_iso8601_timestamp(resultCreatedAt) AT TIME ZONE 'America/Puerto_Rico')
	        AS received_date,
	    date(from_iso8601_timestamp(nullif(collectedDate, '')) AT TIME ZONE 'America/Puerto_Rico')
	        AS raw_collected_date,
	    date(from_iso8601_timestamp(nullif(reportedDate, '')) AT TIME ZONE 'America/Puerto_Rico')
	        AS raw_reported_date,
	    from_hex(replace(nullif(patientId, ''), '-')) AS patient_id,
	    nullif(ageRange, '') AS age_range,
	    nullif(region, '') AS region,
	    testType AS raw_test_type,
        CASE
            WHEN testType IN (
                'Molecular', 'MOLECULAR'
            ) THEN 'Molecular'
            WHEN testType IN (
                'Antigens', 'ANTIGENO'
            ) THEN 'Antígeno'
            WHEN testType IN (
                'Serological', 'Serological IgG Only', 'Total Antibodies', 'SEROLOGICAL'
            ) THEN 'Serológica'
            ELSE 'Otro (¿inválido?)'
        END AS test_type,
	    result,
	    COALESCE(result, '') LIKE '%Positive%' AS positive
	FROM covid_pr_sources.orders_basic_parquet_v2 tests
	INNER JOIN downloads
	    ON downloads.max_downloaded_at = tests.downloadedAt
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
	    THEN least(coalesce(raw_collected_date, DATE '9999-12-31'),
	               coalesce(raw_reported_date, DATE '9999-12-31'))
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
	    THEN greatest(coalesce(raw_collected_date, DATE '0001-01-01'),
	                  coalesce(raw_reported_date, DATE '0001-01-01'))
	    ELSE coalesce(raw_reported_date, raw_collected_date, received_date)
    END AS reported_date
FROM first_clean;
