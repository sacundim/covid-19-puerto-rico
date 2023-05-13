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
        -- Elvis has told me and I have validated that the `orderTestCreatedAt` in
        -- Biostatistics, unlike the similar named field in old Bioportal, is a very
        -- timestamp of when PRDoH actually received the result from the lab.
	    {{ clean_utc_timestamp('orderTestCreatedAt') }}
            AS received_utc,
        {{ utc_to_pr_date('orderTestCreatedAt') }}
            AS received_date
    FROM {{ source('biostatistics', 'tests_v1') }} tests
    LEFT OUTER JOIN {{ ref('expected_test_results') }} results
        ON tests.orderTestResult = results.result
    WHERE orderTestCategory IN ('Covid-19')
    -- IMPORTANT: This prunes partitions
    AND downloaded_date >= cast(date_add('day', -32, current_date) AS VARCHAR)
), date_aux_calculations AS (
    SELECT
        *,
        {{ guess_mistyped_field_diff('year', 'raw_collected_date', 'received_date') }}
            AS guessed_mistyped_year_collected_diff,
        {{ guess_mistyped_field_diff('month', 'raw_collected_date', 'received_date') }}
            AS guessed_mistyped_month_collected_diff,
        {{ guess_mistyped_field_diff('year', 'raw_reported_date', 'received_date') }}
            AS guessed_mistyped_year_reported_diff,
        {{ guess_mistyped_field_diff('month', 'raw_reported_date', 'received_date') }}
            AS guessed_mistyped_month_reported_diff
    FROM first_clean
)
SELECT
    downloaded_date,
    downloaded_at,
    bulletin_date,
    order_test_id,
    patient_id,
    age_range,
    region,
    municipality,
    raw_test_type,
    test_type,
    raw_collected_utc,
    raw_collected_date,
    raw_result_report_utc,
    raw_reported_date,
    result,
    positive,
    received_utc,
    received_date,
    {{ clean_hand_reported_date(
            'raw_collected_date',
            'guessed_mistyped_year_collected_diff',
            'guessed_mistyped_month_collected_diff',
            'raw_reported_date',
            'received_date')
    }} AS collected_date,
    {{ clean_hand_reported_date(
            'raw_reported_date',
            'guessed_mistyped_year_reported_diff',
            'guessed_mistyped_month_reported_diff',
            'raw_collected_date',
            'received_date')
    }} AS reported_date
FROM date_aux_calculations
ORDER BY downloaded_at, raw_collected_utc;