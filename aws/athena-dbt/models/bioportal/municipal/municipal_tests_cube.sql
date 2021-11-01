SELECT
    downloaded_at,
    bulletin_date,
    collected_date,
    reported_date,
    municipality,
    test_type,
    count(*) AS specimens,
    count(*) FILTER (WHERE positive) AS positives
FROM {{ ref('minimal_info_unique_tests') }}
WHERE DATE '2020-03-01' <= collected_date
AND collected_date <= bulletin_date
AND DATE '2020-03-01' <= reported_date
AND reported_date <= bulletin_date
GROUP BY
    downloaded_at,
    bulletin_date,
    collected_date,
    reported_date,
    municipality,
    test_type;
